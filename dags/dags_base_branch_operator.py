from airflow import DAG
import pendulum
from airflow.operators.branch import BaseBranchOperator
from airflow.operators.python import PythonOperator

with DAG(
    dag_id='dags_base_branch_operator',
    start_date=pendulum.datetime(2023,4,1, tz='Asia/Seoul'),
    schedule=None,
    catchup=False
) as dag:
    # Class 생성이 필요함 : BaseBranchOperator 상속 시 choose_branch 함수를 구현해줘야 함.
    # BaseBranchOperator는 CustomBranchOperator의 부모클래스
    class CustomBranchOperator(BaseBranchOperator):
       # 가이드에 제시된 대로 함수명은 choose_branch로 해주어야 함
        def choose_branch(self, context):
            import random
            print(context)
            # context 안에는 각종 정보가 들어있다. 시작시간 등이 들어있으므로 필요하다면 꺼내 사용하면 된다. 
            # context 또한 choose_branch같이 바꾸면 안됨
            
            item_lst = ['A', 'B', 'C']
            selected_item = random.choice(item_lst)
            if selected_item == 'A':
                return 'task_a'
            elif selected_item in ['B','C']:
                return ['task_b','task_c']

    # 객체 정의
    custom_branch_operator = CustomBranchOperator(task_id='python_branch_task')

    
    def common_func(**kwargs):
        print(kwargs['selected'])

    task_a = PythonOperator(
        task_id='task_a',
        python_callable=common_func,
        op_kwargs={'selected':'A'}
    )

    task_b = PythonOperator(
        task_id='task_b',
        python_callable=common_func,
        op_kwargs={'selected':'B'}
    )

    task_c = PythonOperator(
        task_id='task_c',
        python_callable=common_func,
        op_kwargs={'selected':'C'}
    )

    custom_branch_operator >> [task_a, task_b, task_c]