#!/usr/bin/env python2

import argparse
import pprint
import time
from datetime import timedelta

import boto3

USAGE = '''
Restarts an AWS autoscaling group, one instance at a time.
Waits for all instances to be healthy before restarting the next one.
Requires: boto3

Usage: ./restart_asg.py <some-autoscaling-groupname> <region> --ecs_cluster <cluster_name>

'''

DRAIN_WAIT = 600

def get_groups(asg_name, region):
    # returns a list of sets [(capacity, ids), (capacity, ids)] for all matching asgs
    asg_client = boto3.client('autoscaling', region)
    response = asg_client.describe_auto_scaling_groups(
        AutoScalingGroupNames=[asg_name],

    )
    group_info = []
    for group in response['AutoScalingGroups']:
        ids = []
        for instance in group['Instances']:
            if instance['LifecycleState'] == 'InService':
                ids.append(instance['InstanceId'])
        info = (group['DesiredCapacity'], ids)
        group_info.append(info)
    return group_info


def drain_instance(instance_id, ecs_cluster, region):
    client = boto3.client('ecs', region)
    response = client.list_container_instances(cluster=ecs_cluster)
    container_instances = response['containerInstanceArns']
    while response.get('nextToken', None):
        response = client.list_container_instances(cluster=ecs_cluster)
        container_instances.extend(response['containerInstanceArns'])
    all_instances = client.describe_container_instances(
        cluster=ecs_cluster,
        containerInstances=container_instances
    )
    for instance in all_instances['containerInstances']:
        # find the ecs container instance id associated with this ec2 instance id
        if instance['ec2InstanceId'] == instance_id:
            container_instance_id = instance['containerInstanceArn']
            #print("ec2:{} -> ecs:{}".format(instance_id, container_instance_id))
            response = client.update_container_instances_state(
                cluster=ecs_cluster,
                containerInstances=[container_instance_id],
                status='DRAINING',
            )
            #print(pprint.pformat(response))
            if response['failures']:
                raise Exception("Failed to drain: {}".format(response['failures']))
            running_tasks = get_running_tasks(client, ecs_cluster, container_instance_id)
            end_time = time.time() + DRAIN_WAIT
            while running_tasks != 0 and time.time() < end_time:
                time.sleep(30)
                print("Active Tasks: {}".format(running_tasks))
                running_tasks = get_running_tasks(client, ecs_cluster, container_instance_id)
            if time.time() > end_time:
                print("Timed out waiting for instance to drain.")
            return
    raise Exception("Could not find {} in {} ({})".format(instance_id, ecs_cluster, region))


def get_running_tasks(client, ecs_cluster, container_instance_id):
    return client.describe_container_instances(
        cluster=ecs_cluster,
        containerInstances=[container_instance_id]
    )['containerInstances'][0]['runningTasksCount']


def restart_all(asg_name, region, ecs_cluster=None):
    instance_client = boto3.client('ec2', region)
    term_waiter = instance_client.get_waiter('instance_terminated')
    group_info = get_groups(asg_name, region)
    # restart them one-by-one, and wait for each to come back
    for capacity, instance_ids in group_info:
        print("Active Instances: %s" % len(instance_ids))
        for idx, instance_id in enumerate(instance_ids):
            print_sep()
            if ecs_cluster:
                print("Draining: {}".format(instance_id))
                drain_instance(instance_id, ecs_cluster, region)
                print("Drained: {}".format(instance_id))
            try:
                print("Terminating: {}".format(instance_id))
                instance_client.terminate_instances(InstanceIds=[instance_id])
                time.sleep(10)
                term_waiter.wait(InstanceIds=[instance_id])
                print("Terminated: {}".format(instance_id))
            except Exception:
                response = instance_client.describe_instances(InstanceIds=[instance_id])
                pprint.pprint(response)
                state = response['Reservations'][0]['Instances'][0]['State']['Name']
                if state == 'terminated':
                    continue
                else:
                    raise
            wait_for_running(asg_name, region)
            print("Completed {}/{} instances".format(idx + 1, len(instance_ids)))


def wait_for_running(asg_name, region, timeout=10):
    instance_client = boto3.client('ec2', region)
    waiter = instance_client.get_waiter('instance_status_ok')
    end_time = time.time() + (timeout * 60)
    print("Waiting for new instances to be running in %s..." % asg_name)
    while time.time() < end_time:
        group_info = get_groups(asg_name, region)
        for capacity, instance_ids in group_info:
            time.sleep(30)
            # check that desired number of instances have started
            if len(instance_ids) != int(capacity):
                print("Found %s running instances, waiting for %s" % (len(instance_ids), capacity))
                continue
            else:
                # wait for all instances to be running
                print("Waiting for %s healthy instances" % len(instance_ids))
                waiter.wait(InstanceIds=instance_ids, WaiterConfig={'MaxAttempts': 100, 'Delay': 15})
                print("Found %s healthy instances" % len(instance_ids))
                return


def print_sep():
    print("--------------------")


def main(asg_name, region, ecs_cluster):
    start_time = time.time()
    original_instances = [info[1] for info in get_groups(asg_name, region)]
    restart_all(asg_name, region, ecs_cluster)
    final_instances = [info[1] for info in get_groups(asg_name, region)]
    print_sep()
    total_time = (time.time() - start_time)
    pretty_time = timedelta(seconds=int(total_time))
    print("Original Instances: %s" % pprint.pformat(original_instances))
    print("Final Instances: %s" % pprint.pformat(final_instances))
    print("Total Time: %s" % pretty_time)
    print("Done!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=USAGE, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('asg_name')
    parser.add_argument('region')
    parser.add_argument('--ecs_cluster')
    args = parser.parse_args()
    main(args.asg_name, args.region, args.ecs_cluster)
