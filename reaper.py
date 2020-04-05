#!/usr/bin/env python3

import os
import json
from datetime import datetime, timedelta

import click
import boto3
import requests

# the small unit of time over which we report on
# statistics. i.e. when we ask for an average of a value
# it is average over REPORTING_PERIOD_SECS
REPORTING_PERIOD_SECS = 5*60

# this the number of hours we look back when computing
# utilisation
REPORTING_LOOKBACK_TIME_HOURS = 48

SLACK_WEB_HOOK_ENV_VAR = 'SLACK_WEB_HOOK'
SLACK_CHANNEL_ENV_VAR = 'SLACK_CHANNEL'

def slack_send(msg):
  webhook = os.environ.get(SLACK_WEB_HOOK_ENV_VAR)
  channel = os.environ.get(SLACK_CHANNEL_ENV_VAR)

  if webhook is None:
    raise Exception(f'Please set environment variable {SLACK_WEB_HOOK_ENV_VAR}')

  payload = dict(text=msg,
                 username='ec2-reaper',
                 icon_emoji=':robot_face:')
  if channel:
    payload['channel'] = channel

  req = requests.post(webhook, data=json.dumps(payload))
  if req.status_code != 200:
    print('Response:', req.status_code, req.content)

def slack_warn(inst):
  msg = f':warning: Instance {inst.name} ({inst.InstanceId}) has been idle for {inst.idle_period_hours:.2f} hours '
  f'and will be stopped soon.'
  slack_send(msg)

def stop_instance(inst):
  print(f'Stopping instance {inst}')

class Instance(dict):
  def __init__(self, instance_dict, min_cpu_utilisation, min_disk_ops, min_network_packets):
    super().__init__(instance_dict)
    self.__dict__.update(instance_dict)

    self._min_cpu = min_cpu_utilisation
    self._min_disk = min_disk_ops
    self._min_network = min_network_packets

  @property
  def name(self):
    for tagdict in self['Tags']:
      if tagdict['Key'] == 'Name':
        return tagdict['Value']
    return None

  @property
  def state(self):
    return self.State['Name']

  @property
  def is_running(self):
    return self.State['Code'] == 16

  @property
  def cpu_idle_period_hours(self):
    return self.get_idle_period_hours_for_metric('CPUUtilization', 'Percent', self._min_cpu)

  @property
  def network_idle_period_hours(self):
    packets_in_idle = self.get_idle_period_hours_for_metric('NetworkPacketsIn', 'Count', self._min_network)
    packets_out_idle = self.get_idle_period_hours_for_metric('NetworkPacketsOut', 'Count', self._min_network)

    return max(packets_in_idle, packets_out_idle)

  @property
  def disk_idle_period_hours(self):
    # XXX depending on instance type the metric is either
    # DiskRead/WriteOps or EBSRead/WriteOps
    disk_read_idle = self.get_idle_period_hours_for_metric('EBSReadOps', 'Count', self._min_disk)
    if disk_read_idle < 0:
      disk_read_idle = self.get_idle_period_hours_for_metric('DiskReadOps', 'Count', self._min_disk)

    disk_write_idle = self.get_idle_period_hours_for_metric('EBSWriteOps', 'Count', self._min_disk)
    if disk_write_idle < 0:
      disk_write_idle = self.get_idle_period_hours_for_metric('DiskWriteOps', 'Count', self._min_disk)

    return max(disk_read_idle, disk_write_idle)

  @property
  def idle_period_hours(self):
    return min(self.cpu_idle_period_hours,
               self.disk_idle_period_hours,
               self.network_idle_period_hours)
  @property
  def available_metrics(self):
    """
    Poorly implemented only used for quick debugging
    """
    cw = boto3.client('cloudwatch')
    metrics_vec = []
    nexttoken = ''
    done = False
    while not done:
      resp = cw.list_metrics(Namespace='AWS/EC2',
                             Dimensions=[{'Name': 'InstanceId', 'Value':self.InstanceId}])
      for m in resp['Metrics']:
        metrics_vec.append(f'{m["Namespace"]}:{m["MetricName"]}')

      nexttoken = resp.get('NextToken')
      if nexttoken is None:
        done = True

    return metrics_vec

  def get_idle_period_hours_for_metric(self, metricname, unit, idle_threshold):
    """
    Returns the number of hours for which the specified metric was below the idle_threshold.

    If the metric doesn't exist or returns no data points then -1 is returned.
    """
    cw = boto3.client('cloudwatch')
    resp = cw.get_metric_statistics(Namespace='AWS/EC2',
                                    MetricName=metricname,
                                    Dimensions=[{'Name': 'InstanceId', 'Value':self.InstanceId}],
                                    StartTime=datetime.utcnow() - timedelta(hours=REPORTING_LOOKBACK_TIME_HOURS),
                                    EndTime=datetime.utcnow(),
                                    Period=REPORTING_PERIOD_SECS,
                                    Statistics=['Average'],
                                    Unit=unit)

    idle_periods = []
    datapoints = resp['Datapoints']

    if len(datapoints) == 0:
      return -1

    for dp in resp['Datapoints']:
      if dp['Average'] < idle_threshold:
        idle_periods.append(dp['Timestamp'])

    if len(idle_periods):
      idle_periods.sort(reverse=True)


      # find when idle period started by going back in time and finding when was
      # the last time there was more than a reporting period between idleness
      idle_start = idle_periods[-1]
      for cur, prev in zip(idle_periods[:-1], idle_periods[1:]):
        if cur - prev > timedelta(seconds=REPORTING_PERIOD_SECS):
          idle_start = cur

          # stop on the first break in idleness: this is the end of
          # the most recent idle period
          break

      return (idle_periods[0] - idle_start).total_seconds() / 3600
    else:
      # if idle_periods is of length 0 then it means we haven't been idle
      return 0

  def __str__(self):
    return f'{self.name:64s}  {self.InstanceId}  {self.InstanceType:16s} {self.state}'

@click.command()
@click.option('-c', '--min-cpu-utilisation', type=float, default=3,
              help='Minimum CPU utilisation in % per 5 minutes for an instance '
                   'to be considered as active')
@click.option('-d', '--min-disk-ops', type=float, default=1,
              help='Minimum disk operations (read or write) per 5 minutes for an '
                   'instance to be considered active')
@click.option('-n', '--min-network-packets', type=int, default=50,
              help='Minimum (in or out) network packets per 5 minutes for '
                   'an instance to be considered active')
@click.option('-s', '--stop_instance-idle-timeout-hours', type=int, default=6,
              help='After this number of hours of being idle the instance will '
                   'be stop_instance')
@click.option('-w', '--warning-idle-timeout-hours', type=int, default=4,
              help='After this number of hours of being idle a warning will be '
                   'sent that the instance will be stop_instance soon-ish')
@click.option('-v', '--verbose', is_flag=True)
@click.option('--include-stopped', is_flag=True,
              help='Include stopped instances in idleness checks. For debugging '
                   'mostly')
@click.option('--test-slack', type=str, default=None,
              help='When given a test message is sent into slack. Intended for '
                   'verifying slack integration. No other action will be taken')
def main(*args, **kwargs):
  if kwargs['test_slack']:
    slack_send(kwargs['test_slack'])
    return

  kwargs.pop('test_slack')

  kwargs['stop_instance_callback'] = stop_instance
  kwargs['warning_callback'] = slack_warn

  reaper(*args, **kwargs)

def reaper(min_cpu_utilisation,
           min_disk_ops,
           min_network_packets,
           stop_instance_idle_timeout_hours,
           warning_idle_timeout_hours,
           verbose,
           include_stopped,
           warning_callback,
           stop_instance_callback):

  if stop_instance_idle_timeout_hours > REPORTING_LOOKBACK_TIME_HOURS:
    raise Exception(f'stop_instance idle timeout ({stop_instance_idle_timeout_hours}) cannot be longer than '
                    f'{REPORTING_LOOKBACK_TIME_HOURS} hours.')

  if warning_idle_timeout_hours > REPORTING_LOOKBACK_TIME_HOURS:
    raise Exception(f'warning idle timeout ({warning_idle_timeout_hours}) cannot be longer than '
                    f'{REPORTING_LOOKBACK_TIME_HOURS} hours.')
  ec2 = boto3.client('ec2')

  instances_vec = []
  done = False
  nexttoken = ''
  while not done:
    resp = ec2.describe_instances(NextToken=nexttoken)

    instance_args = [min_cpu_utilisation, min_disk_ops, min_network_packets]
    for reservation in resp['Reservations']:
      instances_vec += [Instance(d, *instance_args) for d in reservation['Instances']]

    nexttoken = resp.get('NextToken')
    if nexttoken is None:
      done = True

  if verbose:
    print('Instances')
    print('=========')
    instances_vec.sort(key=lambda i:i.state)
    for inst in instances_vec:
      print(inst)

  if verbose:
    print('')
    print('Idleness')
    print('========')

  warned = 0
  stopped = 0
  checked = 0
  for inst in instances_vec:
    if inst.is_running or include_stopped:
      checked += 1
      if inst.idle_period_hours < stop_instance_idle_timeout_hours:
        if inst.idle_period_hours >= warning_idle_timeout_hours:
          warning_callback(inst)
          warned += 1
      else:
        stop_instance_callback(inst)
        stopped += 1

      if verbose:
        print(f'{inst.name} cpu_idle={inst.cpu_idle_period_hours} '
              f'net_idle={inst.network_idle_period_hours}',
              f'disk_idle={inst.disk_idle_period_hours}')

  print(f'Checked {checked} instances, issued {warned} warnings and stopped {stopped}.')

if __name__ == '__main__':
  main()

