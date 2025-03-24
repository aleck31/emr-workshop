import boto3
import logging

# 设置日志
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    Lambda 函数，用于启动指定的 RDS 实例
    """
    # 设置区域和实例标识符
    region = 'ap-southeast-1'
    db_instance_id = 'test-mysql-public'
    
    try:
        # 创建 RDS 客户端
        rds = boto3.client('rds', region_name=region)
        
        # 检查实例状态
        response = rds.describe_db_instances(DBInstanceIdentifier=db_instance_id)
        status = response['DBInstances'][0]['DBInstanceStatus']
        
        logger.info(f"当前 RDS 实例 {db_instance_id} 状态: {status}")
        
        # 如果实例已停止，则启动它
        if status == 'stopped':
            logger.info(f"正在启动 RDS 实例 {db_instance_id}...")
            rds.start_db_instance(DBInstanceIdentifier=db_instance_id)
            return {
                'statusCode': 200,
                'body': f'RDS 实例 {db_instance_id} 正在启动'
            }
        elif status == 'available':
            logger.info(f"RDS 实例 {db_instance_id} 已经处于运行状态")
            return {
                'statusCode': 200,
                'body': f'RDS 实例 {db_instance_id} 已经处于运行状态'
            }
        else:
            logger.info(f"RDS 实例 {db_instance_id} 当前状态为 {status}，无法执行启动操作")
            return {
                'statusCode': 200,
                'body': f'RDS 实例 {db_instance_id} 当前状态为 {status}，无法执行启动操作'
            }
    
    except Exception as e:
        logger.error(f"启动 RDS 实例时出错: {str(e)}")
        return {
            'statusCode': 500,
            'body': f'启动 RDS 实例时出错: {str(e)}'
        }
