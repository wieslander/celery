# -*- coding: utf-8 -*-
"""
    celery.django
    ~~~~~~~~~~~~~

    Django integration.

"""
from __future__ import absolute_import

import os

# -eof meta-


def setup_loader():
    os.environ.setdefault('CELERY_LOADER',
                          'celery.django.loaders.DjangoLoader')

# Importing this module enables the Celery Django loader.
setup_loader()

from celery import current_app as celery  # noqa
