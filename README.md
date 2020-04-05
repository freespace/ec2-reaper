Introduction
============
This script checks for idleness of running EC2 instances based on CPU
utilisation, disk and network activity. If an instance has been idle for
too long a warning is issued and sometime later the instance will be shutdown.

Slack Integration
=================
The script communicates its intent over slack and relies on the environment
variable `SLACK_WEB_HOOK` to be set and pointing toa valid Slack Incoming
WebHooks. You can create an new web hook by visiting

    https://api.slack.com/incoming-webhooks

The channel messages are posted to can be overridden using the `SLACK_CHANNEL`
environment variable.

