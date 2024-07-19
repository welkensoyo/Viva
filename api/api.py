from bottle import request
from nfty.njson import checkuuid, jc, lc
import uuid
import logging, traceback
from api.clients import Client
from api.users import User, test_users
from nfty.files import FileUpload

logger = logging.getLogger("ApiLogger")
ws_connections = {}


class API:

    def __init__(self, payload, apikey, environ, mode):
        self.mode = mode
        if isinstance(apikey, User):
            self.apikey = 'login'
            self.u = apikey
        else:
            self.u = User()
            self.apikey = apikey
        self.environ = environ
        self.pl = payload
        self.path = request.get('PATH_INFO')
        self.option = self.pl.pop('_method_', '').lower()
        self.option2 = self.pl.pop('_method2_', '').lower()
        self.reference = checkuuid(self.pl.pop('id','') or self.pl.pop('reference',''), version=4) or str(uuid.uuid4())
        try:
            self.request_details = {k: v for k, v in dict(self.environ).items() if isinstance(v, (str, int, list, dict, tuple))}
        except:
            self.request_details = {'mode':'Internal'}
        self.ip = self.request_details.get('HTTP_X_FORWARDED_FOR') or '127.0.0.1'
        self.server = self.request_details.get('REMOTE_ADDR') or '127.0.0.1'
        # if self.apikey == 'login':
        #     self.c = Client(self.u.entities[0] if self.u.entities else '')
        # else:
        #     self.c = Client(self.apikey)
        # self.apikey = self.c.apikey
        # if apikey not in ('login','callback'):
        #     from bottle import abort
        #     abort(401,'Unauthorized...')

    def users(self):
        if self.mode == 'get' and 'ids' in self.pl:
            return self._response(self.u.id, dict(User(u or {}).obj() for u in lc(self.pl['ids'])), 'users')
        return test_users()

    def user(self):
        options = {
            'get': {
                'meta': self.u.dicted,
            },
            'post': {
                'update': self.u.save,
                'login': self.u.login
            }
        }
        return self._call(options, 'user')

    def questions(self):
        options = {
            'get': {
                'all': q.get,
                'random': q.random,
                'category': q.categories
            }
            }
        return self._call(options, 'questions')

    def sparks(self):
        options = {
            'get': {
                'next': self.s.sparks,
                'limit': self.s.sparks_filter
            },
            'post': {
                'sparked': self.s.sparked,
                'snuffed': self.s.snuffed
            }
        }
        return self._call(options, 'questions')

    def files(self):
        if self.option == 'license':
            # x = self.u.update_meta('license', FileUpload(request, self.u).license())
            return FileUpload(request, self.u).license()
        # x = FileUpload(request, self.u).process()
        return {}

    def file(self):
        return self.files()

    def notes(self):
        pass

    def documents(self):
        pass

    def login(self):
        u = User(self.pl['uid'])
        return u.dicted()

    def test(self):
        ws = ws_connections[self.u.id]
        sparks = ws['user'].meta.get('sparks')
        ws['ws'].send(jc({"sparks":sparks}))
        return {'sparks': sparks,
                'user_sparks': self.u.meta.get('sparks')
                }

    def _call(self, options, method):
        x = "Something NFTY didn't work"
        try:
            if self.mode == 'get':
                try:
                    x = options['get'][self.option](self.pl)
                except TypeError as e:
                    x = options['get'][self.option]()
            elif self.mode == 'post':
                x = options['post'][self.option](self.pl)
            elif self.mode == 'delete':
                x = options['delete'][self.option](self.pl)
        except KeyError as e:
            logger.error(traceback.format_exc())
            x = f'Either method did not exist or you forgot to add a method to the API call...'
        except Exception as e:
            logger.error(traceback.format_exc())
        return self._response(self.reference, x, method)

    def _response(self, id, message, method):
        if self.option == 'raw' or self.option2 == 'raw':
            return message
        try:
            message = {'status': message['status'], 'id': id, 'message': message, 'method': method}
        except (TypeError, AttributeError, KeyError):
            message = {'status': False if isinstance(message, str) else True, 'id': id, 'message': message, 'method': method}
        return message
