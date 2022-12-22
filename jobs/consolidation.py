import copy
import datetime
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from utils.elasticsearch.elastic_engine import ESEngine
from utils.logs.log import standardLog

standardLog = standardLog()

class Consolidation:
    def __init__(self, provider_instance):
        self.provider = provider_instance
        self.time = datetime.datetime.utcnow().isoformat()[:-3] + "Z"
        self.timestamp = int(datetime.datetime.timestamp(datetime.datetime.utcnow()) * 1000)

    def consolidation(self, division='region', limit=100, status_option=False):
        try:
            if status_option is True:
                for region in self.provider.regions:
                    for host in region.hosts:
                        if host.status == "disabled":
                            region.hosts.remove(host)
        except Exception as e:
            standardLog.sending_log('error', e).error('host status mapping fail')
            exit()
        standardLog.sending_log('success').info('host status mapping success')


        try:
            past_electronic_cost = 0
            for region in self.provider.regions:
                for host in region.hosts:
                    cpu_ratio = host.used_cpu / host.total_cpu
                    mem_ratio = host.used_memory / host.total_memory
                    if (cpu_ratio + mem_ratio) / 2 == 0:
                        past_electronic_cost += 0
                    else:
                        past_electronic_cost += (200 + ((cpu_ratio + mem_ratio) / 2 * 100))

            placement = list()
            migration_placement = list()

            if division == 'region':
                cluster = self.provider.regions
            elif division == 'zone':
                cluster = self.provider.zones

            for division_cluster in cluster:
                if len(division_cluster.hosts) <= 1:
                    continue

                remain_hosts = list()
                used_hosts = list()
                remain_hosts = division_cluster.hosts.copy()
                used_hosts = sorted(division_cluster.hosts.copy(),
                                    key=lambda x: (x.total_memory, x.total_cpu, x.total_disk))

                total_cpu = 0
                total_memory = 0
                total_disk = 0

                for remain_unit in remain_hosts:
                    total_cpu += round(remain_unit.remain_cpu * limit * 0.01)
                    total_memory += round(remain_unit.remain_memory * limit * 0.01)
                    total_disk += round(remain_unit.remain_disk * limit * 0.01)

                for used_unit in used_hosts:
                    cpu_condition = total_cpu - used_unit.total_cpu
                    memory_condition = total_memory - used_unit.total_memory
                    disk_condition = total_disk - used_unit.used_disk
                    if cpu_condition > 0 and memory_condition > 0 and disk_condition > 0:
                        total_cpu = cpu_condition
                        total_memory = memory_condition
                        total_disk = disk_condition
                        remain_hosts.remove(used_unit)

                used_hosts = list(set(used_hosts) - set(remain_hosts))
                used_hosts = sorted(used_hosts,
                                    key=lambda x: (x.total_memory, x.total_cpu, x.total_disk))

                for used_unit in used_hosts:
                    migration_vms = list()
                    vms = sorted(used_unit.vms,
                                key=lambda x: (x.memory, x.cpu, x.disk))
                    for vm in vms:
                        remain_hosts = sorted(remain_hosts,
                                            key=lambda x: (x.remain_memory,
                                                            x.remain_cpu,
                                                            x.remain_disk), reverse=True)
                        remain_hosts[0].vms.append(vm)
                        used_unit.vms.remove(vm)

                        remain_hosts[0].used_cpu += vm.cpu
                        remain_hosts[0].used_memory += vm.memory
                        remain_hosts[0].used_disk += vm.disk
                        remain_hosts[0].remain_cpu -= vm.cpu
                        remain_hosts[0].remain_memory -= vm.memory
                        remain_hosts[0].remain_disk -= vm.disk

                        used_unit.used_cpu -= vm.cpu
                        used_unit.used_memory -= vm.memory
                        used_unit.used_disk -= vm.disk
                        used_unit.remain_cpu += vm.cpu
                        used_unit.remain_memory += vm.memory
                        used_unit.remain_disk += vm.disk

                        migration_vms.append([used_unit.host_name,
                                            remain_hosts[0].host_name, vm])

                    migration_placement.append(copy.deepcopy(migration_vms))
                    placement.append(copy.deepcopy(self.provider))
        except Exception as e:
            standardLog.sending_log('error', e).error('consolidation migration plan error')
        standardLog.sending_log('success').info('calculate consolidation plan success')

        try:
            if len(placement) == 0:
                if len(remain_hosts) == 1:
                    pass

                memory_total = 0
                memory_total_used = 0
                average_usage = 0
                for remain_unit in remain_hosts:
                    memory_total += remain_unit.total_memory
                    memory_total_used += remain_unit.used_memory

                average_usage = (memory_total_used / memory_total)

                for remain_unit in remain_hosts:
                    if (remain_unit.used_memory / remain_unit.total_memory) > average_usage:
                        used_hosts.append(remain_unit)

                remain_hosts = list(set(remain_hosts) - set(used_hosts))
                remain_hosts = sorted(remain_hosts, key=lambda x: (
                    x.remain_memory, x.remain_cpu, x.remain_disk))
                used_hosts = sorted(used_hosts, key=lambda x: (
                    x.used_memory, x.used_cpu, x.used_disk))

                while len(remain_hosts) >= 1 or len(used_hosts) >= 1:
                    used_hosts = sorted(used_hosts, key=lambda x: (
                        x.used_memory, x.used_cpu, x.used_disk))
                    used_vms = sorted(used_hosts[0].vms, key=lambda x: (
                        x.memory, x.cpu, x.disk))
                    remain_hosts = sorted(remain_hosts, key=lambda x: (
                        x.remain_memory, x.remain_cpu, x.remain_disk), reverse=True)

                    used_hosts[0].vms.remove(used_vms[0])
                    remain_hosts[0].vms.append(used_vms[0])

                    remain_hosts[0].used_cpu += used_vms[0].cpu
                    remain_hosts[0].used_memory += used_vms[0].memory
                    remain_hosts[0].used_disk += used_vms[0].disk
                    remain_hosts[0].remain_cpu -= used_vms[0].cpu
                    remain_hosts[0].remain_memory -= used_vms[0].memory
                    remain_hosts[0].remain_disk -= used_vms[0].disk

                    used_hosts[0].used_cpu -= used_vms[0].cpu
                    used_hosts[0].used_memory -= used_vms[0].memory
                    used_hosts[0].used_disk -= used_vms[0].disk
                    used_hosts[0].remain_cpu += used_vms[0].cpu
                    used_hosts[0].remain_memory += used_vms[0].memory
                    used_hosts[0].remain_disk += used_vms[0].disk

                    migration_placement.append([[used_hosts[0].host_name,
                                                remain_hosts[0].host_name, used_vms[0]]])

                    if (used_hosts[0].used_memory / used_hosts[0].total_memory) <= average_usage:
                        used_hosts.remove(used_hosts[0])
                    if (remain_hosts[0].used_memory /
                            remain_hosts[0].total_memory) >= average_usage:
                        remain_hosts.remove(remain_hosts[0])
                    if len(used_hosts) < 1 or len(remain_hosts) < 1:
                        break

                placement.append(copy.deepcopy(self.provider))
                total_cost = self.consolidation_evaluation(placement, migration_placement,
                                                        past_electronic_cost, possible='no')
                return placement, migration_placement, total_cost
            else:
                total_cost = self.consolidation_evaluation(placement, migration_placement,
                                                        past_electronic_cost)
                return placement, migration_placement, total_cost
        except Exception as e:
            standardLog.sending_log('error', e).error('consolidation migration execution error')
            print(e)
            exit()
        standardLog.sending_log('success').info('calculate consolidation execution success')

    def consolidation_evaluation(self, placement, migration_placement,
                                 past_electronic_cost, possible='yes'):
        total_cost = dict()
        migration_costs = list()
        electronic_costs = list()

        try:
            if possible == 'no':
                migration_costs.append(0)
            else:
                semi_migration_costs = list()
                accumulate = 0
                for migration in migration_placement:
                    accumulate += len(migration)
                    semi_migration_costs.append(
                        (migration_placement.index(migration) + 1) / accumulate)
                max_migration_cost = max(semi_migration_costs)
                for migration_cost in semi_migration_costs:
                    cost = round((migration_cost / max_migration_cost) * 100, 2)
                    migration_costs.append(cost)

            for place in placement:
                for region in place.regions:
                    energy_consumption = 0
                    for host in region.hosts:
                        cpu_ratio = host.used_cpu / host.total_cpu
                        mem_ratio = host.used_memory / host.total_memory
                        if cpu_ratio == 0:
                            continue
                        else:
                            energy_consumption += (200 + ((cpu_ratio + mem_ratio) / 2 * 100))
                    elec_cost = round(100 - ((energy_consumption / past_electronic_cost) * 100), 2)
                electronic_costs.append(elec_cost)

            total_cost['migration cost'] = migration_costs
            total_cost['electronic cost'] = electronic_costs
        except Exception as e:
            standardLog.sending_log('error', e).error('calculate consolidation evaluation error')
            print(e)
            exit()
        standardLog.sending_log('success').info('calculate consolidation evaluation success')

        return total_cost

    def conslidation_to_es(self, placement, migration_placement, total_cost):
        provider_id = placement[0].provider_id
        es = ESEngine()

        count_host = 0
        count_total = 0
        try:
            total_migration_count = 0
            for place in placement:
                shutdown_list = list()
                count_host += 1
                for region in place.regions:
                    for host in region.hosts:
                        es.write_data(index='consolidation_host', data={
                            "datetime": self.time, "timestamp": self.timestamp,
                            "provider_id": provider_id,
                            "consolidation_number": (count_host), "host_name": host.host_name,
                            "host_used_cpu": host.used_cpu, "host_remain_cpu": host.remain_cpu,
                            "host_remain_memory": host.remain_memory,
                            "host_used_memory": host.used_disk,
                            "host_remain_disk": host.remain_disk, "host_used_disk": host.used_disk})

                        if host.used_cpu == 0:
                            shutdown_list.append(host.host_name)
                    count_total += 1
                    total_migration_count += len(migration_placement[count_host - 1])
                    es.write_data(index='consolidation_total', data={
                        "datetime": self.time, "timestamp": self.timestamp,
                        "provider_id": place.provider_id,
                        "consolidation_number": count_total, 'shutdown_host': shutdown_list,
                        "total_migration_count": total_migration_count,
                        'migration_cost': total_cost['migration cost'][count_total - 1],
                        'electronic_cost': total_cost['electronic cost'][count_total - 1]})

            count = 0
            if len(shutdown_list) != 0:
                for migration_count in range(len(migration_placement)):
                    count += 1
                    migration_count += 1
                    for migration_vms in migration_placement[:migration_count]:
                        for migration_vm in migration_vms:
                            es.write_data(index='consolidation_vm', data={
                                "datetime": self.time, "timestamp": self.timestamp,
                                "provider_id": place.provider_id,
                                'consolidation_number': count, 'from_host': migration_vm[0],
                                'to_host': migration_vm[1], 'migration_vm_name': migration_vm[2].vm_name,
                                'migration_vm_cpu': migration_vm[2].cpu,
                                'migration_vm_memory': migration_vm[2].memory,
                                'migration_vm_disk': migration_vm[2].disk})
            else:
                for migration_vms in migration_placement:
                    count += 1
                    for migration_vm in migration_vms:
                        es.write_data(index='consolidation_vm', data={
                            "datetime": self.time, "timestamp": self.timestamp,
                            "provider_id": place.provider_id,
                            'consolidation_number': count, 'from_host': migration_vm[0],
                            'to_host': migration_vm[1], 'migration_vm_name': migration_vm[2].vm_name,
                            'migration_vm_cpu': migration_vm[2].cpu,
                            'migration_vm_memory': migration_vm[2].memory,
                            'migration_vm_disk': migration_vm[2].disk})


        except Exception as e:
            standardLog.sending_log('error', e).error('consolidation result write to ES error')
            print(e)
            exit()
        standardLog.sending_log('success').info('consolidation result write to ES success')