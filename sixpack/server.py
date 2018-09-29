# -*- coding: utf-8 -*-

import os
import re
from socket import inet_aton
import sys
from urllib import unquote

import dateutil.parser
from redis import ConnectionError
from werkzeug.wrappers import Request, Response
from werkzeug.routing import Map, Rule
from werkzeug.exceptions import HTTPException, NotFound
from werkzeug.datastructures import Headers

from . import __version__
from api import participate, convert

from config import CONFIG as cfg
from metrics import init_statsd
from utils import to_bool

try:
    import db
except ConnectionError:
    print "Redis is currently unavailable or misconfigured"
    sys.exit()

from models import Experiment, Client
from utils import service_unavailable_on_connection_error, json_error, json_success


class CORSMiddleware(object):
    """Add Cross-origin resource sharing headers to every request."""

    def __init__(self, app, origin=None):
        self.app = app
        self.redis = self.app.redis
        self.statsd = self.app.statsd
        self.config = self.app.config

        self.origin = origin or cfg.get("cors_origin")
        self.origin_regexp = None
        if self.origin is not None and self.origin != '*':
            self.origin_regexp = re.compile(self.origin.replace("*", "(.*)"))

    def __call__(self, environ, start_response):

        def get_origin(status, headers):
            if self.origin == '*':
                return self.origin
            origin = environ.get("HTTP_ORIGIN", "")
            return origin if self.origin_regexp.match(origin) else "null"

        def add_cors_headers(status, headers, exc_info=None):
            headers = Headers(headers)
            headers.add("Access-Control-Allow-Origin",
                        get_origin(status, headers))
            headers.add("Access-Control-Allow-Headers",
                        cfg.get("cors_headers"))
            headers.add("Access-Control-Allow-Credentials",
                        cfg.get("cors_credentials"))
            headers.add("Access-Control-Allow-Methods",
                        cfg.get("cors_methods"))
            headers.add("Access-Control-Expose-Headers",
                        cfg.get("cors_expose_headers"))
            return start_response(status, headers.to_list(), exc_info)

        if environ.get("REQUEST_METHOD") == "OPTIONS":
            add_cors_headers("200 Ok", [("Content-Type", "text/plain")])
            return [b'200 Ok']

        return self.app(environ, add_cors_headers)


class Sixpack(object):

    def __init__(self, redis_conn):
        self.redis = redis_conn
        self.statsd = init_statsd(cfg) if cfg.get('metrics') else None

        self.config = cfg

        self.url_map = Map([
            Rule('/', endpoint='home'),
            Rule('/_status', endpoint='status'),
            Rule('/participate', endpoint='participate'),
            Rule('/convert', endpoint='convert'),
            Rule('/experiments/<name>', endpoint='experiment_details'),
            Rule('/traffic', endpoint='traffic'),
            Rule('/favicon.ico', endpoint='favicon'),
            Rule('/alt_fractions', endpoint='alt_fractions')
        ])

    def __call__(self, environ, start_response):
        return self.wsgi_app(environ, start_response)

    def wsgi_app(self, environ, start_response):
        request = Request(environ)
        if self.config.get('metrics'):
            dispatcher = self.dispatch_request_with_metrics
        else:
            dispatcher = self.dispatch_request
        response = dispatcher(request)
        return response(environ, start_response)

    def dispatch_request(self, request):
        adapter = self.url_map.bind_to_environ(request.environ)
        try:
            endpoint, values = adapter.match()
            return getattr(self, 'on_' + endpoint)(request, **values)
        except NotFound:
            return json_error({"message": "not found"}, request, 404)
        except HTTPException:
            return json_error({"message": "an internal error has occurred"}, request, 500)

    def _incr_status_code(self, code):
        self.statsd.incr('response_code.{}'.format(code))

    def dispatch_request_with_metrics(self, request):
        adapter = self.url_map.bind_to_environ(request.environ)
        try:
            endpoint, values = adapter.match()
            with self.statsd.timer('{}.response_time'.format(endpoint)):
                response = getattr(self, 'on_' + endpoint)(request, **values)
                self.statsd.incr('{}.count'.format(endpoint))
                self._incr_status_code(response.status_code)
                return response
        except NotFound:
            self._incr_status_code(404)
            return json_error({"message": "not found"}, request, 404)
        except HTTPException:
            self._incr_status_code(500)
            return json_error({"message": "an internal error has occurred"}, request, 500)



    @service_unavailable_on_connection_error
    def on_traffic(self,request):
        experiment_name = request.args.get('experiment')
        traffic_fraction = request.args.get('traffic_fraction')
        if traffic_fraction is None or experiment_name is None:
            return json_error({'message': 'set traffic fail because of missing arguments'}, request, 400)

        traffic_fraction = float(traffic_fraction)
        try:
            exp = Experiment.find(experiment_name, redis=self.redis)
            exp.set_traffic_fraction(traffic_fraction)
            exp.save()
        except ValueError as e:
            return json_error({'message': str(e)}, request, 400)

        resp = {
            'experiment': {
                'name': experiment_name,
                },
            'traffic_fraction': traffic_fraction,
            'status': 'ok'
        }

        return json_success(resp, request)

    @service_unavailable_on_connection_error
    def on_alt_fractions(self, request):
        experiment_name = request.args.get('experiment')
        if experiment_name is None:
            return json_error({'message': 'set alt_fractions fail because of missing experiment name'}, request, 400)

        try:
            exp = Experiment.find(experiment_name, redis=self.redis)
            alt_fractions = []
            for alternabive in exp.alternatives:
                alt_fractions.append(request.args.get(alternabive.name))
            exp.set_alt_fractions(alt_fractions)
            exp.save()
        except ValueError as e:
            return json_error({'message': str(e)}, request, 400)

        resp = {
            'experiment': {
                'name': experiment_name,
            },
            'alt_fraction': exp.alt_fractions,
            'status': 'ok'
        }

        return json_success(resp, request)

    @service_unavailable_on_connection_error
    def on_status(self, request):
        '''
        查看redis状态
        :param request:
        :return:
        '''
        self.redis.ping()# ping成功返回json_sucess,否则返回503 json_error
        return json_success({'version': __version__}, request)


    def on_home(self, request):
        # 显示服务器主页
        dales = """
                 ,-"-.__,-"-.__,-"-..
                ( C>  )( C>  )( C>  ))
               /.`-_-'||`-_-'||`-_-'/
              /-"-.--,-"-.--,-"-.--/|
             ( C>  )( C>  )( C>  )/ |
            (|`-_-',.`-_-',.`-_-'/  |
             `-----++-----++----'|  |
             |     ||     ||     |-'
             |     ||     ||     |
             |     ||     ||     |
              `-_-'  `-_-'  `-_-'
        https://github.com/seatgeek/sixpack"""
        return Response(dales)


    def on_favicon(self, request):
        return Response()

    @service_unavailable_on_connection_error
    def on_convert(self, request):
        if should_exclude_visitor(request):
            return json_success({'excluded': 'true'}, request)

        experiment_name = request.args.get('experiment')
        client_id = request.args.get('client_id')
        kpi = request.args.get('kpi', None)

        if client_id is None or experiment_name is None:
            return json_error({'message': 'missing arguments'}, request, 400)

        dt = None
        if request.args.get("datetime"):
            dt = dateutil.parser.parse(request.args.get("datetime"))

        try:
            alt = convert(experiment_name, client_id, kpi=kpi,#调用转化方法
                          datetime=dt, redis=self.redis)
        except ValueError as e:
            return json_error({'message': str(e)}, request, 400)

        resp = {
            'alternative': {
                'name': alt.name
            },
            'experiment': {
                'name': alt.experiment.name,
            },
            'conversion': {
                'value': None,
                'kpi': kpi
            },
            'client_id': client_id
        }

        return json_success(resp, request)


    @service_unavailable_on_connection_error
    def on_participate(self, request):
        #获取分组列表
        alts = request.args.getlist('alternatives')
        # alt_fractions = request.args.getlist('alt_fractions')
        experiment_name = request.args.get('experiment')#实验名称
        force = request.args.get('force')
        #获取record_force参数，并进行to_bool判断若为[y,ture,yes],返回Ture,否则False
        record_force = to_bool(request.args.get('record_force', 'false'))
        client_id = request.args.get('client_id')#获取client_id
        traffic_fraction = request.args.get('traffic_fraction')#获取参与比例

        if traffic_fraction is not None:
            traffic_fraction = float(traffic_fraction)#若traffic_fraction存在则转为float()
        prefetch = to_bool(request.args.get('prefetch', 'false'))#获取prefetch，并进行to_bool判断若为[y,ture,yes],返回Ture,否则Fals
        if client_id is None or experiment_name is None or alts is None:#三个必要参数，否则返回400
            return json_error({'message': 'missing arguments'}, request, 400)

        dt = None
        if request.args.get("datetime"):#获取datetime参数，转化为datetime.datetime()类型，没有则默认None
            dt = dateutil.parser.parse(request.args.get("datetime"))
        try:
            if should_exclude_visitor(request):#判断user_agent是爬虫或者IP为指定排除的ip,则拒绝参与
                exp = Experiment.find(experiment_name, redis=self.redis)
                if exp.winner is not None:#判断是否已经胜出
                    alt = exp.winner
                else:
                    alt = exp.control#没有胜出，返回分组中的第一个赋值给alt
            else:
                alt = participate(experiment_name, alts, client_id,
                                  force=force, record_force=record_force,
                                  traffic_fraction=traffic_fraction,
                                  # alt_fraction=alt_fractions,
                                  prefetch=prefetch, datetime=dt, redis=self.redis)
        except ValueError as e:
            return json_error({'message': str(e)}, request, 400)

        resp = {
            'alternative': {
                'name': alt.name
            },
            'experiment': {
                'name': alt.experiment.name,
            },
            'client_id': client_id,
            'status': 'ok'
        }

        return json_success(resp, request)

    @service_unavailable_on_connection_error
    def on_experiment_details(self, request, name):
        exp = Experiment.find(name, redis=self.redis)
        if exp is None:
            return json_error({'message': 'experiment not found'}, request, 404)

        return json_success(exp.objectify_by_period('day', True), request)


def should_exclude_visitor(request):
    user_agent = request.args.get('user_agent')
    ip_address = request.args.get('ip_address')

    return is_robot(user_agent) or is_ignored_ip(ip_address)


def is_robot(user_agent):
    """
    :return:若user_agent中能被匹配到设置的robot_regex返回True,否则返回False
    """
    if user_agent is None:
        return False
    regex = re.compile(r"{0}".format(cfg.get('robot_regex')), re.I)
    return regex.search(unquote(user_agent)) is not None


def is_ignored_ip(ip_address):
    # Ignore invalid/local IP addresses
    try:
        inet_aton(unquote(ip_address))#转换IPV4地址字符串（192.168.10.8）成为32位打包的二进制格式
    except:
        return False  # TODO Same as above not sure of default

    return unquote(ip_address) in cfg.get('ignored_ip_addresses')


# Method to run with built-in server
def create_app():
    app = Sixpack(db.REDIS)
    return CORSMiddleware(app)


#  Method to run with gunicorn
def start(environ, start_response):
    return create_app()(environ, start_response)
