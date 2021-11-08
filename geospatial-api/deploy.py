"""
Para crear APIs y funciones o actualizar funciones de lambda
rantonio@arete.ws
"""
from __future__ import print_function
import sys
import json
import boto3
from botocore.exceptions import ClientError
from argparse import ArgumentParser
from logging import getLogger, basicConfig
import configparser

logger = getLogger()

print('leer archivo config')
config = configparser.ConfigParser()
config.read('config.ini')

def set_logger(debug=False):
    """Sets the logger configuration"""
    try:
        logformat = "[ %(levelname)s ] - %(asctime)-15s :: %(message)s"
        basicConfig(format=logformat)
        if debug:
            logger.setLevel(10)

    except Exception as exc:
        print('error', exc)

def get_params():
    """Obtiene los parametros de la linea de comandos"""
    try:
        parser = ArgumentParser(description='ejecucion manual')
        parser.add_argument('artifact', type=str, metavar='<artifact>', default='', help='')
        parser.add_argument('env', type=str, metavar='<env>', default='', help='')
        parser.add_argument('--debug', action="count", default=0, help='')
        args = parser.parse_args()

        return (
            {
                'artifact': args.artifact,
                'env': args.env,
            }, args.debug)

    except Exception as exc:
        print('error', exc)

def publish_new_version(artifact, functions, handler_deploy, env):
    """
    Publicar una nueva version en la funcion(es)
    """
    try:
        client = boto3.client('lambda')
    except ClientError as err:
        print("Fallo al crear el cliente en boto3.\n" + str(err))
        return False
    try:
        for handler in handler_deploy:
            functions_deploy = functions[env][handler]
            if type(functions_deploy) == list:
                for function in functions_deploy:
                    response = client.update_function_code(
                        FunctionName=function,
                        ZipFile=open(artifact, 'rb').read(),
                        Publish=True
                    )
                    print(response)
                    print('funcion actualizada, ', function)
            else:
                response = client.update_function_code(
                    FunctionName=functions_deploy,
                    ZipFile=open(artifact, 'rb').read(),
                    Publish=True
                )
                print(response)
                print('funcion actualizada, ', functions_deploy)
        return 'OK'
    except ClientError as err:
        print("Failed to update function code.\n" + str(err))
        return False
    except IOError as err:
        print("Failed to access " + artifact + ".\n" + str(err))
        return False

def create_new_function(artifact):
    """
    Crear nueva funcion en lambda
    """
    try:
        client = boto3.client('lambda')
    except ClientError as err:
        print("Fallo al crear el cliente en boto3.\n" + str(err))
        return False
    try:
        function_name = config.get('deploy', 'name-function')
        role = config.get('deploy', 'role')
        handler = config.get('deploy', 'handler')
        description = config.get('deploy', 'description')
        timeout = int(config.get('deploy', 'timeout'))
        subnet_ids = json.loads(config.get('deploy', 'subnet-ids'))
        sg = config.get('deploy', 'security-group')
        layers = config.get('deploy', 'layer')
        vpc = config.get('deploy', 'vpc')
        if vpc == 'True':
            response = client.create_function(
                FunctionName=function_name,
                Runtime='python3.7',
                Role=role,
                Handler=handler,
                Code={'ZipFile':open(artifact, 'rb').read()},
                Description=description,
                Timeout=timeout,
                VpcConfig={
                    'SubnetIds': subnet_ids,
                    'SecurityGroupIds': [
                        sg,
                    ]
                },
                Layers=[layers]
            )
        else:
            response = client.create_function(
                FunctionName=function_name,
                Runtime='python3.7',
                Role=role,
                Handler=handler,
                Code={'ZipFile':open(artifact, 'rb').read()},
                Description=description,
                Timeout=timeout,
                Layers=[layers]
            )
        if not 'ResponseMetadata' in response:
            return False
        return True
    except Exception as exc:
        print('error', exc)
        return False

def verificar_existencia_funcion(function_name):
    """
    Verificar la existencia de una funcion en lambda
    """
    try:
        client = boto3.client('lambda')
    except ClientError as err:
        print("Fallo al crear el cliente en boto3.\n" + str(err))
        return False
    try:
        response = client.get_function(
                FunctionName=function_name
            )
        if 'Configuration' in response:
            return True
    except Exception as exc:
        print('error', exc)
        return False

def create_resource():
    """
    Crear un nuevo recurso
    """
    try:
        client = boto3.client('apigateway')
    except ClientError as err:
        print("Fallo al crear el cliente apigateway en boto3.\n" + str(err))
        return False
    try:
        print('Verificar existencia de path')
        path = config.get('deploy', 'path')
        rest_api_id = config.get('deploy', 'rest-api-id')
        path_parent = path
        response = client.get_resources(
            restApiId=rest_api_id
        )
        for ipath in range(len(path.split('/'))):
            for item in response['items']:
                if path_parent == '': 
                    print('Solo existe la raiz del path')
                    path_parent = '/'
                if path_parent == item['path']:
                    print('Ya existe el path')
                    parent_id = item['id']
                    break
            else:
                # Continue if the inner loop wasn't broken.
                path_parent = '/'.join(path_parent.split('/')[:-1])
                continue
            # Inner loop was broken, break the outer.
            break

        print('Crear resource')
        if path_parent == '/':
            resources = path.split('/')[1:]
        else:
            resources = path.split(path_parent)[1].split('/')[1:]
        for resource in resources:
            response = client.create_resource(
                restApiId=rest_api_id,
                parentId=parent_id,
                pathPart=resource
            )
            parent_id = response['id']
        return parent_id
                
    except Exception as exc:
        print('error', exc)
        return False

def put_method(resource_id):
    """Crear metodo post en el recurso creado"""
    try:
        client = boto3.client('apigateway')
    except ClientError as err:
        print("Fallo al crear el cliente apigateway en boto3.\n" + str(err))
        return False
    try:
        rest_api_id = config.get('deploy', 'rest-api-id')
        response = client.put_method(
            restApiId=rest_api_id,
            resourceId=resource_id,
            httpMethod='POST',
            authorizationType='NONE'
        )
        print(response)
        return True
    except Exception as exc:
        print('error', exc)
        return False

def put_method_response(resource_id):
    """Crear metodo post en el recurso creado"""
    try:
        client = boto3.client('apigateway')
    except ClientError as err:
        print("Fallo al crear el cliente apigateway en boto3.\n" + str(err))
        return False
    try:
        rest_api_id = config.get('deploy', 'rest-api-id')
        client.put_method_response(
            restApiId=rest_api_id,
            resourceId=resource_id,
            httpMethod='POST',
            statusCode='200'
        )
        return True
    except Exception as exc:
        print('error', exc)
        return False

def put_integration(resource_id):
    """Integrar la llamada a una funcion en el recurso creado"""
    try:
        client = boto3.client('apigateway')
    except ClientError as err:
        print("Fallo al crear el cliente apigateway en boto3.\n" + str(err))
        return False
    try:
        rest_api_id = config.get('deploy', 'rest-api-id')
        stage_variable = config.get('deploy', 'stage-variable')
        stage_variable = '{stageVariables.'+stage_variable+'}'
        uri = "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:706002245100:function:${}/invocations".format(stage_variable)
        response = client.put_integration(
            restApiId=rest_api_id,
            resourceId=resource_id,
            httpMethod='POST',
            type='AWS',
            integrationHttpMethod='POST',
            uri=uri
        )
        print(response)
        return True
    except Exception as exc:
        print('error', exc)
        return False

def put_integration_response(resource_id):
    """Integrar la llamada a una funcion en el recurso creado"""
    try:
        client = boto3.client('apigateway')
    except ClientError as err:
        print("Fallo al crear el cliente apigateway en boto3.\n" + str(err))
        return False
    try:
        rest_api_id = config.get('deploy', 'rest-api-id')
        response = client.put_integration_response(
            restApiId=rest_api_id,
            resourceId=resource_id,
            httpMethod='POST',
            statusCode='200',
            selectionPattern=''
        )
        print(response)
        return True
    except Exception as exc:
        print('error', exc)
        return False

def create_deployment():
    """Integrar la llamada a una funcion en el recurso creado"""
    try:
        client = boto3.client('apigateway')
    except ClientError as err:
        print("Fallo al crear el cliente apigateway en boto3.\n" + str(err))
        return False
    try:
        rest_api_id = config.get('deploy', 'rest-api-id')
        stage_variable = config.get('deploy', 'stage-variable')
        stage_name = config.get('deploy', 'stage-name')
        name_function = config.get('deploy', 'name-function')
        response = client.create_deployment(
            restApiId=rest_api_id,
            stageName=stage_name,
            variables={
                stage_variable: name_function
            }
        )
        print(response)
        return True
    except Exception as exc:
        print('error', exc)
        return False

def add_permission_lambda_apigateway():
    """
    Agregar permisos a apigateway de acceder a la funcion lambda
    """
    try:
        client = boto3.client('lambda')
    except ClientError as err:
        print("Fallo al crear el cliente en boto3.\n" + str(err))
        return False
    try:
        name_function = config.get('deploy', 'name-function')
        stage_name = config.get('deploy', 'stage-name')
        path = config.get('deploy', 'path')
        statement_id = name_function + '_' + stage_name
        source_arn = 'arn:aws:execute-api:us-east-1:706002245100:x4bixbcudi/*/POST' + path
        response = client.add_permission(
            FunctionName=name_function,
            StatementId=statement_id,
            Action='lambda:InvokeFunction',
            Principal='apigateway.amazonaws.com',
            SourceArn=source_arn
        )
        print(response)
        return True
    except Exception as exc:
        print('error', exc)
        return False

def config_api(env):
    """
    Configurar API en apigateway
    """
    try:
        if env == 'dev':
            print('Crear recurso para completar path')
            resource_id = create_resource()
            if not resource_id:
                return False
            print('Crear metodo POST en recurso')
            if not put_method(resource_id):
                return False
            print('Crear respuesta en el metodo POST')
            if not put_method_response(resource_id):
                return False
            print('Crear put-integration')
            if not put_integration(resource_id):
                return False
            print('Crear put-integration-response')
            if not put_integration_response(resource_id):
                return False
        print('create-deployment')
        if not create_deployment():
            return False
        return True
    except Exception as exc:
        print('error', exc)
        return False

def functions_deploy(artifact, functions, handler_deploy, env):
    """Crear API o publicar nueva version de una funcion"""
    try:
        print('leer handler a actualizar')
        with open(handler_deploy) as f:
            handler_deploy = json.load(f)
            if len(handler_deploy) > 0:
                print('Se encontraron handler a actualizar')
                print('leer las funciones')
                with open(functions) as f:
                    functions = json.load(f)
                if not publish_new_version(artifact, functions, handler_deploy, env):
                    return False
        if config.get('deploy', 'create-function') == 'True':
            print('verifico que no exista la funcion')
            name_function = config.get('deploy', 'name-function')
            if verificar_existencia_funcion(name_function):
                print('La funcion ya existe')
                return False
            print('Crear funcion')
            if not create_new_function(artifact):
                return False
            if not config_api(env):
                return False
            print('Otorgar permisos a funcion desde API gateway')
            if not add_permission_lambda_apigateway():
                return False

        return True
    except Exception as exc:
        print(exc)
        print('Ocurrio un error en functions_deploy')
        return False

    

def main():
    " Obtener los parametros para actualizar la(s) funciones "
    parametros, DEBUG = get_params()
    set_logger(DEBUG)
    if not 'artifact' or not 'env' in parametros:
        sys.exit(1)

    if not functions_deploy(parametros['artifact'], 'functions.json', 'handler_deploy.json', parametros['env']):
        sys.exit(1)        
    

if __name__ == "__main__":
    main()
