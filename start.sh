#!/usr/bin/env bash
set -e
exec gunicorn pantry_app:APP
