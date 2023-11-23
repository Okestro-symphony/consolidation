import kfp
from kfp import dsl
from kfp import components
from kfp.components import func_to_container_op
from elasticsearch import Elasticsearch
import kubernetes.client
client = kfp.Client(host='ip_address')

def cantabile_consolidation() -> bool:
    import os
    os.system('echo -e "\nip_address path.your.api" >> /etc/hosts ') 
    import sys

    sys.path.append('/croffle/cantabile')
    from jobs.consolidation.consolidation import main

    main()
    print('finish')
    return True



cantabile_consolidation_component = components.create_component_from_func(
        func=cantabile_consolidation,                       
        base_image='path/your/image',
        packages_to_install=['requests']    
    )

@dsl.pipeline(
    name="cantabile-consolidation",
)
def cantabile_consolidation_pipeline():
    dsl.get_pipeline_conf().set_image_pull_secrets([kubernetes.client.V1LocalObjectReference(name="public_aiops")])
    cantabile_consolidation_component()


client.create_run_from_pipeline_func(cantabile_consolidation_pipeline, arguments={})


   
kfp.compiler.Compiler().compile(
    pipeline_func=cantabile_consolidation_pipeline,
    package_path='cantabile_consolidation_pipeline.yaml'
)


client.create_recurring_run(
    experiment_id = client.get_experiment(experiment_name="Default").id,
    job_name="cantabile_consolidation",
    description="version: cantabile:consolidation_v1",
    cron_expression="0 10 16 * *",
    pipeline_package_path = "./cantabile_consolidation_pipeline.yaml",
)
