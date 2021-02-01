#!/usr/bin/env python3

from datetime import datetime, timedelta
import boto3

MAX_AVAILABLE = 20
MAX_IN_USE_OVER_200GB = 10
MAX_EBS_SIZE = 10 * 1000

REPORTING_LOOKBACK_TIME_HOURS = 48
REPORTING_PERIOD_SECS = 5 * 60

class Volume(dict):
  def __init__(self, volume_info):
    super().__init__(volume_info)
    self.__dict__.update(volume_info)

  @property
  def name(self):
    if 'Tags' in self:
      for tag in self.Tags:
        if tag['Key'] == 'Name':
          return tag['Value']

    return '<No Name>'

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
      resp = cw.list_metrics(Namespace='AWS/EBS',
                             Dimensions=[{'Name': 'VolumeId', 'Value':self.VolumeId}])
      for m in resp['Metrics']:
        metrics_vec.append(f'{m["Namespace"]}:{m["MetricName"]} D={m["Dimensions"]}')

      nexttoken = resp.get('NextToken')
      if nexttoken is None:
        done = True

    return metrics_vec

  @property
  def time_spent_idle(self):
    # TODO: I can't get this work, no matter what I do I get no datapoints. So for now we can't
    # do idleness base checks :(

    cw = boto3.client('cloudwatch')
    now = datetime.utcnow()
    resp = cw.get_metric_statistics(Namespace='AWS/EBS',
                                    MetricName='VolumeReadOps',
                                    Dimensions=[{'Name': 'VolumeId', 'Value':self.VolumeId}],
                                    StartTime=now - timedelta(hours=REPORTING_LOOKBACK_TIME_HOURS),
                                    EndTime=now.utcnow(),
                                    Period=REPORTING_PERIOD_SECS,
                                    Statistics=['Sum'],
                                    Unit='None')
    return -1

def ebs_reaper(slack_send_func=None):
  """
  Checks for unused (aka available) EBS volumes, warns if:

    1. there are >20 available volumes older than a day
    1. there are >10 in-use volumes over 200GB
    1. there are volumes without read or write activity for 1 week
    1. total amount of ebs is over 10TB

  :param slack_send_func: function to call to send a string into slack. If None then there
         will be no slack integration.
  :return: list of EBSVolume instances to reap
  """

  ec2 = boto3.client('ec2')

  volumes_vec = []
  done = False
  nexttoken = ''
  while not done:
    resp = ec2.describe_volumes(Filters=[{
        'Name': 'status',
        'Values': ['in-use', 'available']
    }])

    nexttoken = resp.get('NextToken')
    if nexttoken is None:
      done = True

    for v in resp['Volumes']:
      volumes_vec.append(Volume(v))

  total_size = sum(v.Size for v in volumes_vec)
  available_volumes = [v for v in volumes_vec if v.State == 'available']
  inuse_volumes_over_200GB = [v for v in volumes_vec if v.State == 'in-use' and v.Size >200]

  for v in volumes_vec:
    print(f'{v.name:64s}  {v.State:10s}  {v.Size:6d} GB')

  print('-'*87)
  print(f'{"Total Size":64s}  {"":10s}  {total_size:6d} GB')

  def warn(msg):
    if slack_send_func:
      slack_send_func(msg)
    print(msg)

  n_available = len(available_volumes)
  if n_available > MAX_AVAILABLE:
    warn(f'⚠️ Number of available volumes ({n_available}) exceeds threshold ({MAX_AVAILABLE}) ⚠️')

  n_inuse_over_200GB = len(inuse_volumes_over_200GB)
  if n_inuse_over_200GB > MAX_IN_USE_OVER_200GB:
    msg = (f'⚠️ Number of in-use volumes > 200GB ({n_inuse_over_200GB}) exceeds '
           f'threshold ({MAX_IN_USE_OVER_200GB}) ⚠️')
    warn(msg)

  if total_size > MAX_EBS_SIZE:
    warn(f'⚠️ Total EBS size ({total_size} GB) exceeds threshold ({MAX_EBS_SIZE} GB) ⚠️')

  # TODO: implement idleness checks here once Volume.time_spent_idle is implemented

def test_smoke():
  ebs_reaper()
