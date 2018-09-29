# -*- coding: utf-8 -*-

from datetime import datetime
from hashlib import sha1
from math import log
from decimal import Decimal
import operator
import random
import re
import redis
import numpy as np

from config import CONFIG as cfg
from db import _key, msetbit, sequential_id, first_key_with_bit_set

# This is pretty restrictive, but we can always relax it later.
VALID_EXPERIMENT_ALTERNATIVE_RE = re.compile(r"^[a-z0-9][a-z0-9\-_]*$", re.I)
VALID_KPI_RE = re.compile(r"^[a-z0-9][a-z0-9\-_]*$", re.I)


class Client(object):

    def __init__(self, client_id, redis=None):
        self.redis = redis
        self.client_id = client_id


class Experiment(object):

    def __init__(self, name, alternatives,
        alt_fractions=None,
        winner=False,
        traffic_fraction=False,
        redis=None):

        if len(alternatives) < 2:
            raise ValueError('experiments require at least two alternatives')
        self.name = name
        self.redis = redis
        self.alternatives = self.initialize_alternatives(alternatives)
        self.kpi = None

        # False here is a sentinal value for "not looked up yet"
        self._winner = winner
        self._alt_fractions = alt_fractions
        self._traffic_fraction = traffic_fraction
        self._sequential_ids = dict()

    def __repr__(self):
        return '<Experiment: {0})>'.format(self.name)

    def objectify_by_period(self, period, slim=False):
        objectified = {
            'name': self.name,
            'period': period,
            'alternatives': [],
            'created_at': self.created_at,
            'traffic_fraction': self.traffic_fraction,
            'excluded_clients': self.excluded_clients(),
            'total_participants': self.total_participants(),
            'total_conversions': self.total_conversions(),
            'description': self.description,
            'has_winner': self.winner is not None,
            'winner': self.winner.name if self.winner is not None else '',
            'is_archived': self.is_archived(),
            'is_paused': self.is_paused(),
            'kpis': list(self.kpis),
            'kpi': self.kpi
        }

        for alternative in self.alternatives:
            objectified_alt = alternative.objectify_by_period(period, slim)
            objectified['alternatives'].append(objectified_alt)

        if slim:
            for key in ['period', 'kpi', 'kpis', 'has_winner']:
                del(objectified[key])

        return objectified

    def initialize_alternatives(self, alternatives):
        for alternative_name in alternatives:
            if not Alternative.is_valid(alternative_name):
                raise ValueError('invalid alternative name')
        # alternative_fractions = {}
        # for i in range(len(alternatives)):
        #     if not Alternative.is_valid(alternatives[i]):
        #         raise ValueError('invalid alternative name')
        #     if alt_fraction:
        #         alternative_fractions[alternatives[i]] = alt_fraction[i]
        #     else:
        #         alternative_fractions[alternatives[i]] = 1/len(alternatives)
        # return [Alternative(n, self, fraction, redis=self.redis) for n, fraction in alternative_fractions.items()]
        return [Alternative(n, self, redis=self.redis) for n in alternatives]
    def save(self):
        pipe = self.redis.pipeline()
        pipe.watch(self.key())
        is_new_record = self.is_new_record()
        try:
            pipe.multi()
            if is_new_record:
                pipe.sadd(_key('e'), self.name)
                pipe.hset(self.key(), 'created_at', datetime.now().strftime("%Y-%m-%d %H:%M"))
                # reverse here and use lpush to keep consistent with using lrange
                for alternative in reversed(self.alternatives):
                    pipe.lpush("{0}:alternatives".format(self.key()), alternative.name)
            for i in range(len(self.alt_fractions)):
                pipe.hset(self.key(),"{0}".format(list(reversed(self.alternatives))[i].name), list(reversed(self.alt_fractions))[i])
            pipe.hset(self.key(), 'traffic_fraction', self._traffic_fraction)
            # pipe.hset(self.key(),)
            pipe.execute()
        except redis.WatchError:
            # another writer has created this experiment and caused
            # our transaction to fail.  assume that everything except
            # the traffic_fraction is the same between the two writers
            # and ensure that the traffic_fraction is updated.
            self.redis.hset(self.key(), 'traffic_fraction', self._traffic_fraction)


    @property
    def control(self):
        return self.alternatives[0]

    @property
    def created_at(self):
        # Note: the split here is to correctly format legacy dates
        try:
            return self.redis.hget(self.key(), 'created_at').split('.')[0]
        except (AttributeError) as e:
            return None

    def get_alternative_names(self):
        return [alt.name for alt in self.alternatives]

    def is_new_record(self):
        return not self.redis.exists(self.key())

    def total_participants(self):
        key = _key("p:{0}:_all:all".format(self.name))
        return self.redis.bitcount(key)

    def participants_by_day(self):
        return self._get_stats('participations', 'days')

    def participants_by_month(self):
        return self._get_stats('participations', 'months')

    def participants_by_year(self):
        return self._get_stats('participations', 'years')

    def total_conversions(self):
        key = _key("c:{0}:_all:users:all".format(self.kpi_key()))
        return self.redis.bitcount(key)

    def conversions_by_day(self):
        return self._get_stats('conversions', 'days')

    def conversions_by_month(self):
        return self._get_stats('conversions', 'months')

    def conversions_by_year(self):
        return self._get_stats('conversions', 'years')

    def _get_stats(self, stat_type, stat_range):
        if stat_type == 'participations':
            stat_type = 'p'
            exp_key = self.name
        elif stat_type == 'conversions':
            stat_type = 'c'
            exp_key = self.kpi_key()
        else:
            raise ValueError("Unrecognized stat type: {0}".format(stat_type))

        if stat_range not in ['days', 'months', 'years']:
            raise ValueError("Unrecognized stat range: {0}".format(stat_range))

        pipe = self.redis.pipeline()

        stats = {}
        search_key = _key("{0}:{1}:{2}".format(stat_type, exp_key, stat_range))
        keys = self.redis.smembers(search_key)
        for k in keys:
            mod = '' if stat_type == 'p' else "users:"
            range_key = _key("{0}:{1}:_all:{2}{3}".format(stat_type, self.name, mod, k))
            pipe.bitcount(range_key)

        redis_results = pipe.execute()
        for idx, k in enumerate(keys):
            stats[k] = float(redis_results[idx])

        return stats

    def update_description(self, description=None):
        if description == '' or description is None:
            self.redis.hdel(self.key(), 'description')
        else:
            self.redis.hset(self.key(), 'description', description)

    @property
    def description(self):
        description = self.redis.hget(self.key(), 'description')
        if description:
            return description.decode("utf-8", "replace")
        else:
            return None

    def reset(self):
        name = self.name
        desc = self.description
        alts = self.get_alternative_names()

        self.delete()

        experiment = Experiment(name, alts, redis=self.redis)
        experiment.update_description(desc)
        experiment.save()

    def delete(self):
        pipe = self.redis.pipeline()
        pipe.srem(_key('e'), self.name)
        pipe.delete(self.key())
        pipe.delete(_key(self.name))
        pipe.delete(_key('e:{0}'.format(self.name)))

        # Consider a 'non-keys' implementation of this
        keys = self.redis.keys('*:{0}:*'.format(self.name))
        for key in keys:
            pipe.delete(key)

        # Delete the KPIs as well
        kpi_keys = self.redis.keys('*:{0}/*'.format(self.name))
        for kpi_key in kpi_keys:
            pipe.delete(kpi_key)

        pipe.execute()

    def archive(self):
        self.redis.hset(self.key(), 'archived', 1)
        self.redis.delete(_key('e:{0}:users'.format(self.name)))

    def is_archived(self):
        """判断是否归档"""
        return self.redis.hexists(self.key(), 'archived')

    def pause(self):
        self.redis.hset(self.key(), 'paused', 1)

    def resume(self):
        self.redis.hdel(self.key(), 'paused')

    def is_paused(self):
        """判断是否暂停"""
        return self.redis.hexists(self.key(), 'paused')

    def convert(self, client, dt=None, kpi=None):
        # print("convert client:",client)
        if self.is_archived():
            raise ValueError('this experiment is archived and can no longer be updated')

        if self.is_paused():
            raise ValueError('this experiment is paused and can not receive updates.')

        if self.is_client_excluded(client):
            raise ValueError('this client was not participating because of excluded')

        alternative = self.existing_alternative(client)
        if not alternative:
            raise ValueError('this client was not participating')

        if kpi is not None:
            if not Experiment.validate_kpi(kpi):
                raise ValueError('invalid kpi name')
            self.add_kpi(kpi)

        if not self.existing_conversion(client):

            alternative.record_conversion(client, dt=dt)

        return alternative

    @property
    def kpis(self):
        return self.redis.smembers("{0}:kpis".format(self.key(include_kpi=False)))

    def set_kpi(self, kpi):
        self.kpi = None

        key = "{0}:kpis".format(self.key())
        if kpi not in self.redis.smembers(key):
            raise ValueError('invalid kpi')

        self.kpi = kpi

    def add_kpi(self, kpi):
        self.redis.sadd("{0}:kpis".format(self.key(include_kpi=False)), kpi)
        self.kpi = kpi

    @property
    def winner(self):
        if self._winner is False:
            self._winner = self.redis.get(self._winner_key)
        if self._winner:
            return Alternative(self._winner, self, redis=self.redis)

    def set_winner(self, alternative_name):
        if alternative_name not in self.get_alternative_names():
            raise ValueError('this alternative is not in this experiment')
        self._winner = alternative_name
        self.redis.set(self._winner_key, alternative_name)

    def reset_winner(self):
        self._winner = None
        self.redis.delete(self._winner_key)

    @property
    def _winner_key(self):
        return "{0}:winner".format(self.key())

    @property
    def alt_fractions(self):
        self._alt_fractions = [float(self.redis.hget(self.key(),'{0}'.format(alt_name.name))) for alt_name in self.alternatives]
        return self._alt_fractions

    def set_alt_fractions(self, alt_fractions):
        if alt_fractions:
            count_miss = alt_fractions.count(None)
            if count_miss == 0 :
                alt_fractions = [round(float(alt_fraction),2) for alt_fraction in alt_fractions]
                sum_fraction = round(np.array(alt_fractions).cumsum()[-1],2)
                if sum_fraction == 1.0 :
                    self._alt_fractions = alt_fractions
                else:
                    raise ValueError('invalid altrnatives fraction ')
            elif count_miss == 1 :
                index = alt_fractions.index(None)
                alt_fractions.pop(index)
                self._alt_fractions = [round(float(alt_fraction), 2) for alt_fraction in alt_fractions]
                sum_fraction = round(np.array(self._alt_fractions).cumsum()[-1], 2)
                if sum_fraction <= 1.0:
                    self._alt_fractions.insert(index,round(1 - sum_fraction, 2))
                else:
                    raise ValueError('invalid altrnatives fraction ')
            else:
                raise ValueError('invalid altrnatives fraction ')
        else:
            self._alt_fractions = [round(float(1.0/len(self.alternatives)),2)] * len(self.alternatives)


    @property
    def traffic_fraction(self):
        if self._traffic_fraction is False:
            try:
                self._traffic_fraction = float(self.redis.hget(self.key(), 'traffic_fraction'))
            except (TypeError, ValueError) as e:
                self._traffic_fraction = 1
        return self._traffic_fraction

    def set_traffic_fraction(self, fraction):
        fraction = float(fraction)
        if not 0 < fraction <= 1:
            raise ValueError('invalid traffic fraction range')

        self._traffic_fraction = fraction

    def sequential_id(self, client):
        """Return the sequential id for this test for the passed in client"""
        if client.client_id not in self._sequential_ids:
            id_ = sequential_id("e:{0}:users".format(self.name), client.client_id)
            # print("sequential_id id_:",id_)
            # print("client.client_id:",client.client_id)
            self._sequential_ids[client.client_id] = id_
            # print("sequential_id self._sequential_ids[client.client_id]:",self._sequential_ids[client.client_id])
        return self._sequential_ids[client.client_id]

    def get_alternative(self, client, dt=None, prefetch=False):
        """Returns and records an alternative according to the following
        precedence:
          1. An existing alternative
          2. A server-chosen alternative
        """

        if self.is_archived() or self.is_paused():
            return self.control

        if self.is_client_excluded(client):
            return self.control

        chosen_alternative = self.existing_alternative(client)
        if not chosen_alternative:
            chosen_alternative, participate = self.choose_alternative(client)
            if participate and not prefetch:
                chosen_alternative.record_participation(client, dt=dt)

        return chosen_alternative

    def exclude_client(self, client):
        key = _key("e:{0}:excluded".format(self.name))
        self.redis.setbit(key, self.sequential_id(client), 1)

    def is_client_excluded(self, client):
        """判断是否是拒绝用户"""
        key = _key("e:{0}:excluded".format(self.name))
        return self.redis.getbit(key, self.sequential_id(client))

    def excluded_clients(self):
        key = _key("e:{0}:excluded".format(self.name))
        return self.redis.bitcount(key)

    def existing_alternative(self, client):
        if self.is_client_excluded(client):
            return None

        alts = self.get_alternative_names()
        # print("existing_alternative alts:",alts)
        keys = [_key("p:{0}:{1}:all".format(self.name, alt)) for alt in alts]
        # print("existing_alternative keys:",keys)
        # print("existing_alternative self.sequential_id(client):",self.sequential_id(client))
        altkey = first_key_with_bit_set(keys=keys, args=[self.sequential_id(client)])
        # print("excluded_clients altkey:",altkey)
        if altkey:
            idx = keys.index(altkey)
            # print("existing_alternative keys.index:",idx)
            return Alternative(alts[idx], self, redis=self.redis)

        return None

    def choose_alternative(self, client):
        # fixme
        rnd = random.random()
        # print("choose_alternative rnd:",rnd)
        if rnd >= self.traffic_fraction:
            self.exclude_client(client)
            return self.control, False
        return self._alt_fraction_choice(client), True

    def _alt_fraction_choice(self,client):
        rand = random.random()
        try:#首先从redis中获取获取alt_fractions,并进行比较选择
            idx = list(np.cumsum(np.array(self.alt_fractions))<rand).index(False)
            return self.alternatives[idx]
        except (TypeError, ValueError):#若表中没有，则选择原有方式，根据client_id求出hash值对总分组数取模随机选择
            return self._uniform_choice(client)

    # Ported from https://github.com/facebook/planout/blob/master/planout/ops/random.py
    def _uniform_choice(self, client):
        idx = self._get_hash(client) % len(self.alternatives)
        print(idx)
        return self.alternatives[idx]

    def _get_hash(self, client):
        # print("_get_hash self.name:",self.name)
        # print("_get_hash client.client_id:",client.client_id)
        salty = "{0}.{1}".format(self.name, client.client_id)

        # We're going to take the first 7 bytes of the client UUID
        # because of the largest integer values that can be represented safely
        # with Sixpack client libraries
        # More Info: https://github.com/seatgeek/sixpack/issues/132#issuecomment-54318218
        hashed = sha1(salty).hexdigest()[:7]
        return int(hashed, 16)

    def existing_conversion(self, client):
        alts = self.get_alternative_names()
        keys = [_key("c:{0}:{1}:users:all".format(self.kpi_key(), alt)) for alt in alts]
        # print("existing_conversion keys:",keys)
        altkey = first_key_with_bit_set(keys=keys, args=[self.sequential_id(client)])
        # print("existing_conversion altkey:",altkey)
        if altkey:
            idx = keys.index(altkey)
            return Alternative(alts[idx], self, redis=self.redis)

        return None

    def kpi_key(self):
        if self.kpi is not None:
            return "{0}/{1}".format(self.name, self.kpi)
        else:
            return self.name

    def key(self, include_kpi=True):
        if include_kpi:
            return _key("e:{0}".format(self.kpi_key()))
        else:
            return _key("e:{0}".format(self.name))

    @classmethod
    def find(cls, experiment_name,
        redis=None):
        """
        判断实验是否存在，不存在抛出值异常
        """
        if not redis.sismember(_key("e"), experiment_name):
            raise ValueError('experiment does not exist')
        return cls(experiment_name,
                   Experiment.load_alternatives(experiment_name, redis),
                   redis=redis)

    @classmethod
    def find_or_create(cls, experiment_name, alternatives,
        alt_fractions = None,
        traffic_fraction=None,
        redis=None):

        if len(alternatives) < 2:
            raise ValueError('experiments require at least two alternatives')

        # Traffic fraction can change at any time, so it needs to be
        # checked.

        if traffic_fraction is None:
            traffic_fraction = 1

        is_update = False
        try:
            experiment = Experiment.find(experiment_name, redis=redis)
            is_update = True
            # print("old_traffic:",experiment.traffic_fraction)
        except ValueError:
            experiment = cls(experiment_name, alternatives, redis=redis)
            # TODO: I want to revisit this later.
            experiment.set_traffic_fraction(traffic_fraction)
            experiment.set_alt_fractions(alt_fractions)
            experiment.save()

        # Only check traffic fraction if the experiment is being updated 
        # and the traffic fraction actually changes.
        # if is_update and experiment.traffic_fraction != traffic_fraction:
        if is_update and experiment.traffic_fraction is None:
            experiment.set_traffic_fraction(traffic_fraction)
            experiment.save()

        # Make sure the alternative options are correct. If they are not,
        # raise an error.
        if sorted(experiment.get_alternative_names()) != sorted(alternatives):
            raise ValueError('experiment alternatives have changed. please delete in the admin')

        return experiment

    @staticmethod
    def all_names(redis=None):
        return redis.smembers(_key('e'))

    @staticmethod
    def all(exclude_archived=True, exclude_paused=True, redis=None):
        experiments = []
        keys = redis.smembers(_key('e'))

        for key in keys:
            experiment = Experiment.find(key, redis=redis)
            if experiment.is_archived() and exclude_archived:
                continue
            if experiment.is_paused() and exclude_paused:
                continue
            experiments.append(experiment)
        return experiments

    @staticmethod
    def archived(redis=None):
        experiments = Experiment.all(exclude_archived=False, exclude_paused=True, redis=redis)
        return [exp for exp in experiments if exp.is_archived()]

    @staticmethod
    def paused(redis=None):
        experiments = Experiment.all(exclude_archived=True, exclude_paused=False, redis=redis)
        return [exp for exp in experiments if exp.is_paused()]

    @staticmethod
    def load_alternatives(experiment_name, redis=None):
        """
        :param experiment_name:
        :param redis:
        :return: 实验对应所有分组
        """
        key = _key("e:{0}:alternatives".format(experiment_name))
        return redis.lrange(key, 0, -1)

    @staticmethod
    def is_valid(experiment_name):
        return (isinstance(experiment_name, basestring) and
                VALID_EXPERIMENT_ALTERNATIVE_RE.match(experiment_name) is not None)

    @staticmethod
    def validate_kpi(kpi):
        return (isinstance(kpi, basestring) and
                VALID_KPI_RE.match(kpi) is not None)


class Alternative(object):

    def __init__(self, name, experiment, alt_fraction=None, redis=None):
        self.name = name
        self.experiment = experiment
        self.redis = redis
        self._alt_fraction = alt_fraction

    def __repr__(self):
        return "<Alternative {0} (Experiment {1})>".format(repr(self.name), repr(self.experiment.name))

    @property
    def alt_fraction(self):
        try:
            self._alt_fraction = float(self.redis.hget(self.__key(), self.name))
        except (TypeError, ValueError):
            self._alt_fraction = "random"
        return self._alt_fraction

    def objectify_by_period(self, period, slim=False):

        if slim:
            return self.name

        PERIOD_TO_METHOD_MAP = {
            'day': {
                'participants': self.participants_by_day,
                'conversions': self.conversions_by_day
            },
            'month': {
                'participants': self.participants_by_month,
                'conversions': self.conversions_by_month
            },
            'year': {
                'participants': self.participants_by_year,
                'conversions': self.conversions_by_year
            },
        }

        data = []
        conversion_fn = PERIOD_TO_METHOD_MAP[period]['conversions']
        participants_fn = PERIOD_TO_METHOD_MAP[period]['participants']

        conversions = conversion_fn()
        participants = participants_fn()

        dates = sorted(list(set(conversions.keys() + participants.keys())))
        for date in dates:
            _data = {
                'conversions': conversions.get(date, 0),
                'participants': participants.get(date, 0),
                'date': date
            }
            data.append(_data)

        objectified = {
            'name': self.name,
            'data': data,
            'alt_fraction': self.alt_fraction,
            'conversion_rate': float('%.2f' % (self.conversion_rate() * 100)),
            'is_control': self.is_control(),
            'is_winner': self.is_winner(),
            'test_statistic': self.g_stat(),
            'participant_count': self.participant_count(),
            'completed_count': self.completed_count(),
            'confidence_level': self.confidence_level(),
            'confidence_interval': self.confidence_interval()
        }

        return objectified

    def is_control(self):
        return self.experiment.control.name == self.name

    def is_winner(self):
        winner = self.experiment.winner
        return winner and winner.name == self.name

    def participant_count(self):
        key = _key("p:{0}:{1}:all".format(self.experiment.name, self.name))
        return self.redis.bitcount(key)

    def participants_by_day(self):
        return self._get_stats('participations', 'days')

    def participants_by_month(self):
        return self._get_stats('participations', 'months')

    def participants_by_year(self):
        return self._get_stats('participations', 'years')

    def completed_count(self):
        key = _key("c:{0}:{1}:users:all".format(self.experiment.kpi_key(), self.name))
        return self.redis.bitcount(key)

    def conversions_by_day(self):
        return self._get_stats('conversions', 'days')

    def conversions_by_month(self):
        return self._get_stats('conversions', 'months')

    def conversions_by_year(self):
        return self._get_stats('conversions', 'years')

    def _get_stats(self, stat_type, stat_range):
        if stat_type == 'participations':
            stat_type = 'p'
            exp_key = self.experiment.name
        elif stat_type == 'conversions':
            stat_type = 'c'
            exp_key = self.experiment.kpi_key()
        else:
            raise ValueError("Unrecognized stat type: {0}".format(stat_type))

        if stat_range not in ['days', 'months', 'years']:
            raise ValueError("Unrecognized stat range: {0}".format(stat_range))

        stats = {}

        pipe = self.redis.pipeline()

        search_key = _key("{0}:{1}:{2}".format(stat_type, exp_key, stat_range))
        keys = self.redis.smembers(search_key)

        for k in keys:
            name = self.name if stat_type == 'p' else "{0}:users".format(self.name)
            range_key = _key("{0}:{1}:{2}:{3}".format(stat_type, exp_key, name, k))
            pipe.bitcount(range_key)

        redis_results = pipe.execute()
        for idx, k in enumerate(keys):
            stats[k] = float(redis_results[idx])

        return stats

    def record_participation(self, client, dt=None):
        """Record a user's participation in a test along with a given variation"""
        if dt is None:
            date = datetime.now()
        else:
            date = dt

        experiment_key = self.experiment.name

        pipe = self.redis.pipeline()

        pipe.sadd(_key("p:{0}:years".format(experiment_key)), date.strftime('%Y'))
        pipe.sadd(_key("p:{0}:months".format(experiment_key)), date.strftime('%Y-%m'))
        pipe.sadd(_key("p:{0}:days".format(experiment_key)), date.strftime('%Y-%m-%d'))

        pipe.execute()

        keys = [
            _key("p:{0}:_all:all".format(experiment_key)),
            _key("p:{0}:_all:{1}".format(experiment_key, date.strftime('%Y'))),
            _key("p:{0}:_all:{1}".format(experiment_key, date.strftime('%Y-%m'))),
            _key("p:{0}:_all:{1}".format(experiment_key, date.strftime('%Y-%m-%d'))),
            _key("p:{0}:{1}:all".format(experiment_key, self.name)),
            _key("p:{0}:{1}:{2}".format(experiment_key, self.name, date.strftime('%Y'))),
            _key("p:{0}:{1}:{2}".format(experiment_key, self.name, date.strftime('%Y-%m'))),
            _key("p:{0}:{1}:{2}".format(experiment_key, self.name, date.strftime('%Y-%m-%d'))),
        ]
        # fixme
        msetbit(keys=keys, args=([self.experiment.sequential_id(client), 1] * len(keys)))

    def record_conversion(self, client, dt=None):
        # print("record_conversion client:",client)
        """Record a user's conversion in a test along with a given variation"""
        if dt is None:
            date = datetime.now()
        else:
            date = dt

        experiment_key = self.experiment.kpi_key()
        # print("record_conversion experiment_key:",experiment_key)

        pipe = self.redis.pipeline()

        pipe.sadd(_key("c:{0}:years".format(experiment_key)), date.strftime('%Y'))
        pipe.sadd(_key("c:{0}:months".format(experiment_key)), date.strftime('%Y-%m'))
        pipe.sadd(_key("c:{0}:days".format(experiment_key)), date.strftime('%Y-%m-%d'))

        pipe.execute()

        keys = [
            _key("c:{0}:_all:users:all".format(experiment_key)),
            _key("c:{0}:_all:users:{1}".format(experiment_key, date.strftime('%Y'))),
            _key("c:{0}:_all:users:{1}".format(experiment_key, date.strftime('%Y-%m'))),
            _key("c:{0}:_all:users:{1}".format(experiment_key, date.strftime('%Y-%m-%d'))),
            _key("c:{0}:{1}:users:all".format(experiment_key, self.name)),
            _key("c:{0}:{1}:users:{2}".format(experiment_key, self.name, date.strftime('%Y'))),
            _key("c:{0}:{1}:users:{2}".format(experiment_key, self.name, date.strftime('%Y-%m'))),
            _key("c:{0}:{1}:users:{2}".format(experiment_key, self.name, date.strftime('%Y-%m-%d'))),
        ]
        sequence_id_ = self.experiment.sequential_id(client)
        # print("record_conversion sequence_id_:",sequence_id_)
        # print("record_conversion len(keys):",len(keys))
        # print("record_conversion [sequence_id_, 1] * len(keys):",[sequence_id_, 1] * len(keys))
        msetbit(keys=keys, args=([sequence_id_, 1] * len(keys)))

    def conversion_rate(self):
        try:
            return self.completed_count() / float(self.participant_count())
        except ZeroDivisionError:
            return 0

    def g_stat(self):
        # http://en.wikipedia.org/wiki/G-test

        if self.is_control():
            return 'N/A'

        control = self.experiment.control

        alt_conversions = self.completed_count()
        control_conversions = control.completed_count()
        alt_failures = self.participant_count() - alt_conversions
        control_failures = control.participant_count() - control_conversions

        total_conversions = alt_conversions + control_conversions

        if total_conversions < 20:
            # small sample size of conversions, see where it goes for a bit
            return 'N/A'

        total_participants = self.participant_count() + control.participant_count()

        expected_control_conversions = control.participant_count() * total_conversions / float(total_participants)
        expected_alt_conversions = self.participant_count() * total_conversions / float(total_participants)
        expected_control_failures = control.participant_count() - expected_control_conversions
        expected_alt_failures = self.participant_count() - expected_alt_conversions

        try:
            g_stat = 2 * (      alt_conversions * log(alt_conversions / expected_alt_conversions) \
                        +   alt_failures * log(alt_failures / expected_alt_failures) \
                        +   control_conversions * log(control_conversions / expected_control_conversions) \
                        +   control_failures * log(control_failures / expected_control_failures))

        except (ValueError, ZeroDivisionError):
            return 0

        return round(g_stat, 2)

    def z_score(self):
        if self.is_control():
            return 'N/A'

        control = self.experiment.control
        ctr_e = self.conversion_rate()
        ctr_c = control.conversion_rate()

        e = self.participant_count()
        c = control.participant_count()

        try:
            std_dev = pow(((ctr_e / pow(ctr_c, 3)) * ((e * ctr_e) + (c * ctr_c) - (ctr_c * ctr_e) * (c + e)) / (c * e)), 0.5)
            z_score = ((ctr_e / ctr_c) - 1) / std_dev
            return z_score
        except ZeroDivisionError:
            return 0

    def g_confidence_level(self):
        # g stat is approximated by chi-square, we will use
        # critical values from chi-square distribution with one degree of freedom

        g_stat = self.g_stat()
        if g_stat == 'N/A':
            return g_stat

        ret = ''
        if g_stat == 0.0:
            ret = 'No Change'
        elif g_stat < 3.841:
            ret = 'No Confidence'
        elif g_stat < 6.635:
            ret = '95% Confidence'
        elif g_stat < 10.83:
            ret = '99% Confidence'
        else:
            ret = '99.9% Confidence'

        return ret

    def z_confidence_level(self):
        z_score = self.z_score()
        if z_score == 'N/A':
            return z_score

        z_score = abs(round(z_score, 3))

        ret = ''
        if z_score == 0.0:
            ret = 'No Change'
        elif z_score < 1.96:
            ret = 'No Confidence'
        elif z_score < 2.57:
            ret = '95% Confidence'
        elif z_score < 3.27:
            ret = '99% Confidence'
        else:
            ret = '99.9% Confidence'

        return ret

    def confidence_level(self, conf_type="g"):
        if conf_type == "z":
            return self.z_confidence_level()
        else:
            return self.g_confidence_level()

    def confidence_interval(self):
        try:
            # 80% confidence
            p = self.conversion_rate()
            return pow(p * (1 - p) / self.participant_count(), 0.5) * 1.28 * 100
        except ZeroDivisionError:
            return 0

    def __key(self):
        return _key("e:{0}".format(self.experiment.name))

    def key(self):
        return _key("{0}:{1}".format(self.experiment.name, self.name))

    @staticmethod
    def is_valid(alternative_name):
        return (isinstance(alternative_name, basestring) and
                VALID_EXPERIMENT_ALTERNATIVE_RE.match(alternative_name) is not None)
