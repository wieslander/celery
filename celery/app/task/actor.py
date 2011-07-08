from copy import copy
from celery.app.task import BaseTask, TaskType


class Exposed(object):
    actor = None

    def __init__(self, fun, actor=None):
        self.fun = fun
        self.actor = actor

    def __copy__(self):
        return self.__class__(self.fun, self.actor)

    def __call__(self, *args, **kwargs):
        return self.fun(self.actor, *args, **kwargs)

    def call(self, *args, **kwargs):
        return self.actor.apply_async(
                    args=("call", self.__name__, args, kwargs)).get()

    def cast(self, *args, **kwargs):
        self.actor.apply_async(args=("cast", self.__name__, args, kwargs))

    def __repr__(self):
        return "<Exposed %s of %r>" % (self.__name__, self.actor)


def expose(fun):
    return type(fun.__name__, (Exposed, ), {"__name__": fun.__name__,
                                            "__doc__": fun.__doc__,
                                            "__module__": fun.__module__})(fun)



class ActorType(TaskType):

    def __new__(cls, name, bases, attrs):
        print("CLS: %r NAME: %r BASES: %r ATTRS: %r" % (cls, name, bases,
            attrs))
        abstract = getattr(cls, "abstract", attrs.get("abstract"))
        cls = super(ActorType, cls).__new__(cls, name, bases, attrs)
        cls.exposed = {}
        if not abstract and not cls._prepared:
            for name, sym in vars(cls).iteritems():
                print("NAME: %r, SYM: %r" % (name, sym))
                if isinstance(sym, Exposed):
                    sym.__name__ = name
                    sym.actor = cls
                    cls.exposed[name] = sym
            cls._prepared = True
        return cls

    def __repr__(cls):
        if cls.name:
            return "<Actor: %s of %s>" % (cls.name, cls.app)
        return "<class Actor of %s>" % (cls.app, )


class BaseActor(BaseTask):
    __metaclass__ = ActorType

    _prepared = False
    abstract = True
    accept_magic_kwargs = False

    def __init__(self, *args, **kwargs):
        exposed = {}
        print("INITITITIT")
        for name, method in self.exposed.iteritems():
            print("NAME: %r, METHOD: %r" % (name, method))
            method = copy(method)
            method.actor = self
            setattr(self, name, method)
        self.exposed = exposed

    def run(self, type, method, args, kwargs):
        return self.typemap[type](method, *args, **kwargs)

    def handle_cast(self, method, *args, **kwargs):
        print("CAST!")
        self._apply_exposed(method, args, kwargs)

    def handle_call(self, method, *args, **kwargs):
        return self._apply_exposed(method, args, kwargs)

    def cast(self, method, args, kwargs):
        self.apply_async(args=(method, args, kwargs, True))

    def call(self, method, args, kwargs):
        return self.apply_async(args=(method, args, kwargs, False))

    def handle_info(self, *args, **kwargs):
        raise NotImplementedError("handle_info")

    def _apply_exposed(self, method, args, kwargs):
        try:
            m = getattr(self, method)
        except AttributeError:
            raise NotImplementedError(method)
        return m(*args, **kwargs)

    @property
    def typemap(self):
        return {"cast": self.handle_cast,
                "call": self.handle_call,
                "info": self.handle_info}
