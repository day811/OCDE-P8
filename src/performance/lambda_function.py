import os
import sys
import json
import time
import logging
import boto3
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient
from dateutil import parser

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================

log_level = os.getenv('LOG_LEVEL', 'INFO').upper()

# Détecter si on est en ECS / Lambda
IS_ECS = os.getenv('SUBNET_ID') is not None
IS_LAMBDA = os.getenv('AWS_LAMBDA_FUNCTION_NAME') is not None

handlers = [
    logging.StreamHandler(sys.stdout),  # Toujours vers stdout
]

# En local/dev, ajouter fichier
if not IS_ECS and not IS_LAMBDA:
    os.makedirs('logs', exist_ok=True)
    handlers.append(
        logging.FileHandler('logs/performance.log', mode='a', encoding='utf-8')
    )

logging.basicConfig(
    level=getattr(logging, log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=handlers
)

logger = logging.getLogger(__name__)

# Si en Lambda, ajouter les logs CloudWatch via Watchtower (optionnel mais recommandé)
try:
    if IS_LAMBDA:
        import watchtower
        cloudwatch_logs = boto3.client('logs')
        logger.addHandler(
            watchtower.CloudWatchLogHandler(
                log_group='/aws/lambda/greencoop-performance-benchmark',
                stream_name='performance-stream',
                client=cloudwatch_logs
            )
        )
except ImportError:
    logger.warning("Watchtower non disponible. Logs CloudWatch désactivés.")

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
    if os.getenv('DOCKMODE') or os.getenv('SUBNET_ID'):
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
    target_date_str = event.get('target_date') if event else "2024-10-05"
    
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

def push_metric_to_cloudwatch(station_id, duration_ms, doc_count, date_ref):
    """Envoie les métriques vers CloudWatch."""
    namespace = os.getenv('CLOUDWATCH_NAMESPACE', 'Greencoop/Forecast')
    
    metric_data = [
        {
            'MetricName': 'QueryExecutionTime',
            'Dimensions': [
                {'Name': 'StationId', 'Value': station_id},
                {'Name': 'Environment', 'Value': os.getenv('ENV', 'prod')}
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
                {'Name': 'Environment', 'Value': os.getenv('ENV', 'prod')}
            ],
            'Timestamp': datetime.now(timezone.utc),
            'Value': doc_count,
            'Unit': 'Count',
            'StorageResolution': 60
        }
    ]
    
    try:
        cloudwatch = get_cloudwatch_client()
        cloudwatch.put_metric_data(Namespace=namespace, MetricData=metric_data)
        logger.debug(f"Métriques CloudWatch envoyées pour {station_id}")
    except Exception as e:
        logger.error(f"Erreur lors de l'envoi CloudWatch pour {station_id}: {str(e)}")

# ============================================================================
# HANDLER LAMBDA
# ============================================================================

def lambda_handler(event, context):
    """Point d'entrée principal de la Lambda."""
    logger.info(f"Début du benchmark. Event: {json.dumps(event) if event else 'Aucun event'}")
    
    try:
        # 1. Configuration de la date
        start_date, end_date = get_target_date_range(event)
        logger.info(f"Période cible: {start_date.isoformat()} à {end_date.isoformat()}")
        
        # 2. Connexion DB
        obs_coll, stations_coll = get_mongo_collection()
        logger.info("Connexion MongoDB établie")
        
        # 3. Récupération de la liste des stations actives
        stations = list(stations_coll.find({}, {'id_station': 1}))
        logger.info(f"Nombre de stations à tester: {len(stations)}")
        
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
                push_metric_to_cloudwatch(station_id, duration, count, start_date)
                
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
    import sys
    
    # Construire l'event de test
    test_event = {}
    if len(sys.argv) > 1:
        test_event['target_date'] = sys.argv[1]
    
    logger.info("Mode développement local - Démarrage du test")
    
    # Exécuter le handler
    result = lambda_handler(test_event, None)
    
    # Afficher le résultat
    logger.info(f"Résultat: {result}")
    print(json.dumps(json.loads(result['body']), indent=2))
