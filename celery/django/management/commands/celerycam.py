"""

Shortcut to the Django snapshot service.

"""
from __future__ import absolute_import

from celery.bin import celeryev

from celery.django.app import app
from celery.django.management.base import CeleryCommand

ev = celeryev.EvCommand(app=app)


class Command(CeleryCommand):
    """Run the celery curses event viewer."""
    options = (CeleryCommand.options
             + ev.get_options()
             + ev.preload_options)
    help = 'Takes snapshots of the clusters state to the database.'

    def handle(self, *args, **options):
        """Handle the management command."""
        options["camera"] = "celery.django.snapshot.Camera"
        ev.run(*args, **options)
