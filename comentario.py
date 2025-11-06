import boto3
import uuid
import os
import json
from datetime import datetime

def lambda_handler(event, context):
    # Entrada (json)
    print(event)
    tenant_id = event['body']['tenant_id']
    texto = event['body']['texto']
    nombre_tabla = os.environ["TABLE_NAME"]
    nombre_bucket = os.environ["BUCKET_NAME"]
    
    # Proceso
    uuidv1 = str(uuid.uuid1())
    timestamp = datetime.utcnow().isoformat()
    
    comentario = {
        'tenant_id': tenant_id,
        'uuid': uuidv1,
        'detalle': {
          'texto': texto
        },
        'timestamp': timestamp
    }
    
    # Guardar en DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(nombre_tabla)
    response = table.put_item(Item=comentario)
    
    # Guardar en S3 (Ingesta Push)
    s3_client = boto3.client('s3')
    # Estructura de carpetas: tenant_id/año/mes/día/uuid.json
    date_obj = datetime.utcnow()
    s3_key = f"{tenant_id}/{date_obj.year}/{date_obj.month:02d}/{date_obj.day:02d}/{uuidv1}.json"
    
    try:
        s3_client.put_object(
            Bucket=nombre_bucket,
            Key=s3_key,
            Body=json.dumps(comentario, ensure_ascii=False, indent=2),
            ContentType='application/json'
        )
        print(f"Comentario guardado en S3: s3://{nombre_bucket}/{s3_key}")
    except Exception as e:
        print(f"Error al guardar en S3: {str(e)}")
        # No falla la función si S3 falla, pero registra el error
    
    # Salida (json)
    print(comentario)
    return {
        'statusCode': 200,
        'comentario': comentario,
        'response': response,
        's3_location': f"s3://{nombre_bucket}/{s3_key}"
    }
