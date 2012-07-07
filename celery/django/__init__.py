# -*- coding: utf-8 -*-
"""
    celery.django
    ~~~~~~~~~~~~~

    Django integration.

"""
from __future__ import absolute_import
from __future__ import with_statement

import errno
import os
import sys

# -eof meta-


def setup_loader():
    os.environ.setdefault('CELERY_LOADER',
                          'celery.django.loaders.DjangoLoader')


def _in_django():
    try:
        from django import settings
    except ImportError:
        pass
    else:
        if settings.configured:
            return True
    return False


def _needs_setup():
    import django
    return django.VERSION < (1, 4)


def _setup_django(cwd=None):
    if _needs_setup():
        from django.core.management import setup_environ
        from importlib import import_module
        setup_environ(import_module(os.environ.get('DJANGO_SETTINGS_MODULE',
                                                   'settings')))


def _has_manage_py():
    try:
        with open('manage.py') as fh:
            for line in fh:
                if 'django.core' in line:
                    return True
    except Exception:
        pass
    return False


def _maybe_relaunch_with_manage(argv=sys.argv):
    if os.environ.get('DJANGO_SETTINGS_MODULE'):
        return
    if _has_manage_py():
        return os.execv(sys.executable, ['manage.py'] + argv)





from celery import current_app as celery  # noqa
