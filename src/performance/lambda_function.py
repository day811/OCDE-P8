import os
import sys
import json
import time
import logging
import boto3
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient
from dateutil import parser
import argparse

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================

log_level = os.getenv('LOG_LEVEL', 'INFO').upper()

# Détecter si on est en ECS
RUN_MODE = os.getenv('RUN_MODE','shell')
IS_ECS = RUN_MODE == 'ecs'

IS_LAMBDA = os.getenv('AWS_LAMBDA_FUNCTION_NAME') is not None

# Configurer le root logger
logger = logging.getLogger()
logger.setLevel(getattr(logging, log_level))

# Supprimer les handlers par défaut
for h in list(logger.handlers):
    logger.removeHandler(h)

# Formatter commun
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Handler stdout (toujours)
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

# Handler fichier (local/Docker uniquement, pas Lambda)
if not IS_LAMBDA:
    os.makedirs('logs', exist_ok=True)
    file_handler = logging.FileHandler('logs/performance.log', mode='a', encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

# Logger du module
logger = logging.getLogger(__name__)

logger.info(f"Mode d'exécution: {RUN_MODE}")
logger.info(f"Configuration file name : {str(IS_LAMBDA)}")


# ============================================================================
# INITIALISATION DES CLIENTS
# ============================================================================

_cloudwatch_client = None

def get_cloudwatch_client():
    """
    Retourne le client CloudWatch initialisé avec la région AWS correcte.
    
    En ECS : utilise le rôle IAM de la tâche (pas de clés nécessaires).
    En local : utilise AWS_ACCESS_KEY_ID et AWS_SECRET_ACCESS_KEY du .env.
    """
    global _cloudwatch_client
    if _cloudwatch_client is None:
        region = os.getenv('AWS_REGION', 'eu-west-3')
        
        if IS_ECS:
            # En ECS : le rôle IAM de la tâche est utilisé automatiquement
            logger.info(f"Initialisation CloudWatch en ECS (rôle IAM) - région: {region}")
            _cloudwatch_client = boto3.client('cloudwatch', region_name=region)
        else:
            # En local : utiliser les clés d'accès depuis .env
            access_key = os.getenv('AWS_ACCESS_KEY_ID')
            secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
            session_token = os.getenv('AWS_SESSION_TOKEN')
            
            if not access_key or not secret_key:
                logger.warning(
                    "AWS_ACCESS_KEY_ID ou AWS_SECRET_ACCESS_KEY non défini. "
                    "CloudWatch sera inaccessible en mode local."
                )
                _cloudwatch_client = None
                return None
            
            logger.info(f"Initialisation CloudWatch en local - région: {region}")
            
            # Création explicite avec les clés
            if session_token:
                _cloudwatch_client = boto3.client(
                    'cloudwatch',
                    region_name=region,
                    aws_access_key_id=access_key,
                    aws_secret_access_key=secret_key,
                    aws_session_token=session_token
                )
            else:
                _cloudwatch_client = boto3.client(
                    'cloudwatch',
                    region_name=region,
                    aws_access_key_id=access_key,
                    aws_secret_access_key=secret_key
                )
    
    return _cloudwatch_client

# ============================================================================
# FONCTIONS MÉTIER
# ============================================================================

def get_mongo_collection():
    """Établit la connexion à MongoDB et retourne la collection observations."""
    uri = os.getenv('MONGODB_URI')
    if RUN_MODE != 'shell':
        uri =  str(uri).replace('@localhost:', '@mongodb:')
    else:
        uri =  str(uri).replace('@mongodb:', '@localhost:')
    db_name = os.getenv('DATABASE_NAME', 'greencoopforecast')
    
    if not uri:
        raise ValueError("MONGODB_URI n'est pas défini")
    
    # LOG MASQUÉ (pour sécurité)
    masked_uri = "xxxx:yyyyy@" + uri.split('@')[1] if '@' in uri else uri
    logger.info(f"Connexion à MongoDB: {masked_uri}")
    logger.info(f"DATABASE_NAME: {db_name}")
    
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    db = client[db_name]
    return db.observations, db.stations

def get_target_date_range(event):
    """
    Détermine la plage de date cible (début et fin de journée UTC).
    Priorité : 1. Paramètre 'target_date' dans l'event. 2. Hier (J-1).
    """
    target_date_str = event.get('target_date') if event else None
    if not target_date_str:
        target_date_str = os.getenv('TARGET_DATE',None)

    if target_date_str:
        try:
            # Accepte le format YYYY-MM-DD
            target_dt = datetime.strptime(target_date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            logger.info(f"Date cible fournie: {target_date_str}")
        except ValueError:
            logger.error(f"Format de date invalide: {target_date_str}. Attendu: YYYY-MM-DD")
            raise
    else:
        # Par défaut : Hier
        target_dt = datetime.now(timezone.utc) - timedelta(days=1)
        logger.info(f"Aucune date fournie. Utilisation de la veille: {target_dt.date()}")
    
    # Définir le début (00:00:00) et la fin (23:59:59) de la journée cible
    start_date = target_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = target_dt.replace(hour=23, minute=59, second=59, microsecond=999999)
    
    return start_date, end_date

def measure_query_performance(collection, station_id, start_date, end_date):
    """
    Exécute la requête de référence et mesure le temps d'exécution.
    Retourne le temps en millisecondes et le nombre de documents trouvés.
 """
    query = {
        'id_station': station_id,
        'dh_utc': {
            '$gte': start_date,
            '$lte': end_date
        }
    }
    logger.info(f"Requête générée: {query}/{start_date}/{end_date}")
    
    # Démarrage du chronomètre haute résolution
    start_time = time.perf_counter()
    
    # Exécution de la requête (on force la matérialisation en liste pour inclure le temps de fetch)
    results = list(collection.find(query))
    
    # Arrêt du chronomètre
    end_time = time.perf_counter()
    
    duration_ms = (end_time - start_time) * 1000
    doc_count = len(results)
    
    return duration_ms, doc_count

def push_metric_to_cloudwatch(station_id, duration_ms, doc_count, date_ref, metrix_env):
    """Envoie les métriques vers CloudWatch."""
    namespace = os.getenv('CLOUDWATCH_NAMESPACE', 'Greencoop/Forecast')
    
    metric_data = [
        {
            'MetricName': 'QueryExecutionTime',
            'Dimensions': [
                {'Name': 'StationId', 'Value': station_id},
                {'Name': 'Environment', 'Value': metrix_env},
                {'Name': 'Reference Date', 'Value': date_ref}
            ],
            'Timestamp': datetime.now(timezone.utc),
            'Value': duration_ms,
            'Unit': 'Milliseconds',
            'StorageResolution': 60
        },
        {
            'MetricName': 'DocumentsRetrieved',
            'Dimensions': [
                {'Name': 'StationId', 'Value': station_id},
                {'Name': 'Environment', 'Value': metrix_env},
                {'Name': 'Reference Date', 'Value': date_ref}
            ],
            'Timestamp': datetime.now(timezone.utc),
            'Value': doc_count,
            'Unit': 'Count',
            'StorageResolution': 60
        }
    ]
    
    try:
        # connexion test in any cases
        cloudwatch = get_cloudwatch_client()
    
        if metrix_env != 'no-log':
            cloudwatch.put_metric_data(Namespace=namespace, MetricData=metric_data)
            logger.info(f"Métriques CloudWatch {metrix_env} envoyées pour {station_id}")
        else:
            logger.info(f"Métriques CloudWatch non envoyées (no-log) pour {station_id}")
            logger.debug(f"Métriques data :\n {metric_data}")
            
            
    except Exception as e:
        logger.error(f"Erreur lors de l'envoi CloudWatch pour {station_id}: {str(e)}")

# ============================================================================
# HANDLER LAMBDA
# ============================================================================

def lambda_handler(event, context):
    """Point d'entrée principal de la Lambda."""
    print("DEBUG: Je suis un print simple")  
    logger.info(f"Début du benchmark. Event: {json.dumps(event) if event else 'Aucun event'}")
    
    try:
        
        logger.info(f"Mode d'exécution: {RUN_MODE}")
        
        # 1. Configuration de la date
        start_date, end_date = get_target_date_range(event)
        logger.info(f"Période cible: {start_date.isoformat()} à {end_date.isoformat()}")
        
        # 2. Connexion DB
        obs_coll, stations_coll = get_mongo_collection()
        logger.info("Connexion MongoDB établie")
        
        # 3. Récupération de la liste des stations actives
        stations = list(stations_coll.find({}, {'id_station': 1}))
        logger.info(f"Nombre de stations à tester: {len(stations)}")

        # 4. Récupération de l'environnement des metrix
        metrix_env = event.get('metrix_env')
        if not metrix_env:
            metrix_env = os.getenv('METRIX_ENV', 'no-log')
        
        logger.info(f"Environnement des metrix : {metrix_env}")
        if not stations:
            logger.warning("Aucune station trouvée dans la base")
            return {
                'statusCode': 204,
                'body': json.dumps({'message': 'Aucune station à mesurer'})
            }
        
        results_summary = []
        
        # 4. Boucle de mesure par station
        for station in stations:
            station_id = station.get('id_station')
            if not station_id:
                logger.warning(f"Station sans id_station trouvée: {station}")
                continue
                
            try:
                # Mesure
                duration, count = measure_query_performance(obs_coll, station_id, start_date, end_date)
                
                # Envoi métrique
                push_metric_to_cloudwatch(station_id, duration, count, start_date.isoformat(), metrix_env)
                
                logger.info(f"Station {station_id}: {duration:.2f}ms ({count} docs)")
                
                results_summary.append({
                    'station': station_id,
                    'duration_ms': round(duration, 2),
                    'doc_count': count
                })
            except Exception as e:
                logger.error(f"Erreur lors du benchmark de la station {station_id}: {str(e)}")
                results_summary.append({
                    'station': station_id,
                    'error': str(e)
                })
            
        logger.info(f"Benchmark terminé. {len(results_summary)} stations traitées")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Benchmark terminé avec succès',
                'date_target': start_date.isoformat(),
                'stations_tested': len(results_summary),
                'details': results_summary
            })
        }
        
    except Exception as e:
        logger.error(f"Erreur critique dans la Lambda: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

# ============================================================================
# POINT D'ENTRÉE LOCAL (DÉVELOPPEMENT)
# ============================================================================

if __name__ == '__main__':
    """
    Permet de tester localement:
    
    # Test avec date par défaut (hier)
    python src/performance/lambda_function.py
    
    # Test avec date spécifique
    python src/performance/lambda_function.py 2025-12-03
    """
    parser = argparse.ArgumentParser(description='Load JSONL data into MongoDB (normalized schema)')
    parser.add_argument('--target-date', default="2024-10-05",
                       help='MongoDB connection URI')
    parser.add_argument('--metrix-env',default = None,
                       help='Metrix environment (no-log/test/prod)')
    
    args = parser.parse_args()    
    # Construire l'event de test
    test_event = {}
    test_event['target_date'] = args.target_date
    test_event['metrix_env'] = args.metrix_env
    
    # Exécuter le handler
    result = lambda_handler(test_event, None)
    
    # Afficher le résultat
    logger.info(f"Résultat: {result}")
    print(json.dumps(json.loads(result['body']), indent=2))
