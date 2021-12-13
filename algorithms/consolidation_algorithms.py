# -*- coding: utf-8 -*-
import copy
import uuid

from ..server_objects import *
from ..migration_objects import *


def migrate(from_host, to_host, vm, consolidation):
    from_host.pop_vm(vm)
    to_host.add_vm(vm)
    consolidation.add_migration(
        Migration(from_host_id=from_host.id, from_host_name=from_host.name, to_host_name=to_host.name,
                  to_host_id=to_host.id, vm_name=vm.name,
                  vm=vm))


class ConsolidationAlgorithm:
    def __init__(self, cluster=None, total_consolidation=None):
        self.cluster = cluster
        self.total_consolidation = total_consolidation

    def ffd_planner_v1(self):
        """
        ffd 알고리즘을 끝까지 수행하는 함수
        :return: consolidation object
        """
        consolidation = Consolidation()  ####전체 이동정보를 저장할 객체 생성
        cluster = copy.deepcopy(self.cluster)
        cpu_sorted_hosts = cluster.sort_hosts_by_vcpu_used()  #### cpu 사용량에 따라 정렬
        for i in range(len(cluster.hosts)):
            from_hostname, from_host = cpu_sorted_hosts[i]  ###vm을 migration할 호스트 선정
            if len(from_host.vms) == 0:  ####host내 vm개수가 0이면 다음 host로 넘어간다
                from_host.is_idle = False
                continue
            cpu_sorted_vms = from_host.sort_vms_by_vcpu(reverse=True)  ###host내에서 vm을 크기에 따라 정렬
            for _, vm in cpu_sorted_vms:  ####vm을 선택
                for j in range(1, len(cluster.hosts) + 1):
                    to_hostname, to_host = cpu_sorted_hosts[-j]  ###vcpu사용량이 많은 host부터 확인한다

                    condition1 = from_host.retrieve_vcpu_used() < to_host.retrieve_vcpu_used()  ###조건1: 이동할 host의 vCpu의 개수가 이전 host의 vCpu보다 많음
                    condition2 = vm.vcpu <= to_host.retrieve_vcpu_free()  ###조건2: 이동할 host내 vCpu사용량에 여유가 있어야함
                    condition3 = vm.vmemory <= to_host.retrieve_memory_free()  ###조건3: 이동할 host에 memory 여유가 있어야함

                    if condition1 and condition2 and condition3:
                        migrate(from_host, to_host, vm, consolidation)  # migration 을 실행하고 배치정보에 저장
                        cpu_sorted_hosts = cluster.sort_hosts_by_vcpu_used()  ###이동한 vcpu값을 가지고 host 재정렬
                        if len(from_host.vms) == 0:
                            consolidation.number_server_shutdown += 1
                            from_host.is_idle = False
                        break
        consolidation.save_cluster_status(cluster)
        return consolidation

    def ffd_planner_v2(self):
        """
        ffd를 수행하며 서버가 하나 꺼질때마다 저장을 해준다
        :return: self.total_consolidation에 해당 배치 정보들이 저장된다
        """
        consolidation = Consolidation()  ####이동정보들을 저장할 객체 생성
        cluster = copy.deepcopy(self.cluster)
        cpu_sorted_hosts = cluster.sort_hosts_by_vcpu_used()  #### cpu 사용량에 따라 정렬

        consolidation.save_cluster_status(cluster)
        self.total_consolidation.add_consolidation(
            '{}server off migration plan'.format(consolidation.number_server_shutdown),
            copy.deepcopy(consolidation))

        for i in range(len(cluster.hosts)):
            from_hostname, from_host = cpu_sorted_hosts[i]  ###vm을 migration할 호스트 선정

            if len(from_host.vms) == 0:  ####host내 vm개수가 0이면 다음 host로 넘어간다
                if from_host.is_idle: #서버가 꺼져있는데 켜져있다고 인식되면
                    consolidation.number_server_shutdown += 1  ###이미꺼져있는 서버이므로 꺼져있는서버개수에 추가
                    from_host.is_idle = False
                continue

            cpu_sorted_vms = from_host.sort_vms_by_vcpu(reverse=True)  ###host내에서 vm을 크기에 따라 정렬 (큰거부터 작은것 순서로)

            for _, vm in cpu_sorted_vms:  ####vm을 선택
                for j in range(1, len(cluster.hosts)+1):
                    to_hostname, to_host = cpu_sorted_hosts[-j]  ###vcpu사용량이 많은 host부터 확인한다

                    condition1 = from_host.retrieve_vcpu_used() < to_host.retrieve_vcpu_used()  ###조건1: 이동할 host의 vCpu의 개수가 이전 host의 vCpu보다 많음
                    condition2 = vm.vcpu <= to_host.retrieve_vcpu_free()  ###조건2: 이동할 host내 vCpu사용량에 여유가 있어야함
                    condition3 = vm.vmemory <= to_host.retrieve_memory_free()  ###조건3: 이동할 host에 memory 여유가 있어야함

                    if condition1 and condition2 and condition3:
                        migrate(from_host, to_host, vm, consolidation)  # migration 을 실행하고 배치정보에 저장
                        cpu_sorted_hosts = cluster.sort_hosts_by_vcpu_used()  ###이동한 vcpu값을 가지고 host 재정렬

                        if len(from_host.vms) == 0:
                            consolidation.number_server_shutdown += 1  ###서버가 꺼졌으므로 꺼진 서버 개수에 추가
                            from_host.is_idle = False
                            consolidation.uuid = uuid.uuid4()
                            consolidation.save_cluster_status(cluster)
                            self.total_consolidation.add_consolidation(
                                '{}server off migration plan'.format(consolidation.number_server_shutdown),
                                copy.deepcopy(consolidation))  ####서버가 꺼진시점의 마이그레이션을 저장해준다
                        break

    def load_balancer_v1(self):
        """
        vcpu 를 rebalancing하는 알고리즘
        :return:
        """
        for _, consolidation in self.total_consolidation.consolidations.items():  ### 최적배치 계획을 하나씩 가져와서 수정

            cluster = consolidation.cluster  ####최적배치 계획속 클러스터 상태 가져오기
            cpu_sorted_hosts = cluster.sort_hosts_by_vcpu_used(reverse=True)  #####cpu 사용량에 따라 역순  정렬
            for i in range(len(cluster.hosts)):
                from_hostname, from_host = cpu_sorted_hosts[i]  ####vm 을 migration할 호스트 선정
                if len(from_host.vms) == 0:  ####꺼져있는 서버이면 넘어간다
                    continue
                cpu_sorted_vms = from_host.sort_vms_by_vcpu(reverse=True)  ### host내에서 vm들을 정렬
                for vmname, vm in cpu_sorted_vms:
                    for j in range(1,len(cluster.hosts)+1):
                        to_hostname, to_host = cpu_sorted_hosts[-j]  ###cpu사용량이 적은 순서대로 선택

                        condition1 = from_host.retrieve_vcpu_used() > to_host.retrieve_vcpu_used() + vm.vcpu  ###조건1 이동할 host와 이동할 vm vcpu 개수의 합이 이전 host 의 vCpu 보다 적음
                        condition2 = vm.vcpu <= to_host.retrieve_vcpu_free()  ###조건2: 이동할 host내 vCpu 사용량에 여유가 있어야함
                        condition3 = vm.vmemory <= to_host.retrieve_memory_free()  ### 이동할 host에 memory여유가 있어야함
                        condition4 = len(to_host.vms) != 0  ####꺼져있는 서버는 건드리지말자

                        if condition1 and condition2 and condition3 and condition4:
                            migrate(from_host, to_host, vm, consolidation)  # migration 을 실행하고 배치정보에 저장
                            cpu_sorted_hosts = cluster.sort_hosts_by_vcpu_used(reverse=True)### 이동한 vcpu값을 가지고 host역순 재정렬
                            break
            consolidation.energy_consumption = cluster.calculate_energy_consumption()
            consolidation.workload_stability = 100
            consolidation.save_cluster_status(cluster)

    def load_balancer_v2(self):
        """
        vcpu, 안정성을 기준으로 rebalancing하는 알고리즘
        :return:
        """
        for plan_name, consolidation in self.total_consolidation.consolidations.items():  ### 최적배치 계획을 하나씩 가져와서 수정
            cluster = consolidation.cluster  ####최적배치 계획속 클러스터 상태 가져오기
            cpu_sorted_hosts = cluster.sort_hosts_by_vcpu_used(reverse=True)  ##### cpu 사용량에 따라 정렬
            for i in range(len(cluster.hosts)):
                from_hostname, from_host = cpu_sorted_hosts[i]  ####vm 을 migration할 호스트 선정
                if len(from_host.vms) == 0:  ####꺼져있는 서버이면 넘어간다
                    continue
                cpu_sorted_vms = from_host.sort_vms_by_vcpu(reverse=True)  ### host내에서 vm들을 정렬
                for _, vm in cpu_sorted_vms:
                    candidate_list = [] ###넘겨짚을수 있는 후보들이 있다면

                    for j in range(len(cluster.hosts)):
                        to_hostname, to_host = cpu_sorted_hosts[-j]  ###cpu사용량이 적은 순서대로 선택

                        condition1 = from_host.retrieve_vcpu_used() > to_host.retrieve_vcpu_used() + vm.vcpu  ###조건1 이동할 host와 이동할 vm vcpu 개수의 합이 이전 host 의 vCpu 보다 적음
                        condition2 = vm.vcpu <= to_host.retrieve_vcpu_free()  ###조건2: 이동할 host내 vCpu 사용량에 여유가 있어야함
                        condition3 = vm.vmemory <= to_host.retrieve_memory_free()  ### 이동할 host에 memory여유가 있어야함
                        condition4 = len(to_host.vms) != 0  ####꺼져있는 서버는 건드리지말자

                        if condition1 and condition2 and condition3 and condition4:
                            candidate = copy.deepcopy(cluster)
                            candidate.hosts[from_hostname].pop_vm(vm)
                            candidate.hosts[to_hostname].add_vm(vm)
                            candidate_list.append((to_hostname, candidate.calculate_workload_stability())) ###가야하는 host 와 workload_stability의 값을 같이 넣어준다.

                    if candidate_list:
                        candidate_list.sort(key=lambda candidate: candidate[1], reverse=True) ##workload_stability가 높은 순서대로 정렬
                        current_stability = cluster.calculate_workload_stability()
                        final_candidate = candidate_list[0] ##최종후보 선정
                        if final_candidate[1] > current_stability:
                            migrate(from_host, to_host, vm, consolidation)
                            cpu_sorted_hosts = cluster.sort_hosts_by_vcpu_used(reverse=True)
            consolidation.workload_stability = cluster.calculate_workload_stability()
            consolidation.energy_consumption = cluster.calculate_energy_consumption()
            consolidation.save_cluster_status(cluster)

    def final_consolidation(self):
        try:
            self.ffd_planner_v2()
            self.load_balancer_v2()
            return self.total_consolidation
        except:
            try:
                self.ffd_planner_v2()
                self.load_balancer_v1()
                return self.total_consoldidation
            except:
                self.ffd_planner_v2()
                return self.total_consolidation