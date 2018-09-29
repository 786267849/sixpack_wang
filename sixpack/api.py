# -*- coding: utf-8 -*-

from models import Experiment, Alternative, Client
from config import CONFIG as cfg


def participate(experiment, alternatives, client_id,
    force=None,
    record_force=False,
    # alt_fraction=None,
    traffic_fraction=None,
    prefetch=False,
    datetime=None,
    redis=None):

    exp = Experiment.find_or_create(experiment, alternatives, traffic_fraction=traffic_fraction, redis=redis)

    alt = None
    if force and force in alternatives:
        alt = Alternative(force, exp, redis=redis)

        if record_force:
            client = Client(client_id, redis=redis)
            alt.record_participation(client, datetime)

    elif not cfg.get('enabled', True):
        alt = exp.control
    elif exp.winner is not None:
        alt = exp.winner
    else:
        client = Client(client_id, redis=redis)
        alt = exp.get_alternative(client, dt=datetime, prefetch=prefetch)

    return alt


def convert(experiment, client_id,
    kpi=None,
    datetime=None,
    redis=None):
    """"""
    exp = Experiment.find(experiment, redis=redis)#查找实验，得到实验对象

    if cfg.get('enabled', True):#判断是否为启用状态
        client = Client(client_id, redis=redis)#创建client对象
        alt = exp.convert(client, dt=datetime, kpi=kpi)#调用exp对象的convert方法
    else:
        alt = exp.control

    return alt
