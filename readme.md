

### Projet : **Intégration de données hospitalières et impact du COVID-19**


### **Objectifs**

1. Construire un pipeline de données en temps réel pour traiter les données sur Kafka.
2. Intégrer et nettoyer des données.
3. Gérer les valeurs manquantes et aberrantes.
4. Calculer des métriques.
5. Visualiser les tendances et extraire des insights pour communiquer efficacement les résultats.

---

### **Sources de données**

- **hospital-utilization-trends** (données en streaming via Kafka).  
- **in-hospital-mortality-trends-by-diagnosis-type** (données batch).  
- **in-hospital-mortality-trends-by-health-category** (données batch).

---

### **Technologies utilisées**

- **Apache Spark** :
  - Spark Streaming pour le traitement en temps réel.
  - PySpark pour les transformations et analyses des données.
- **Kafka** :
  - Ingestion des données en streaming pour les tendances d'utilisation hospitalière.
- **Python** :
  - Nettoyage, traitement et visualisation des données.
  - Bibliothèques : pandas, matplotlib, seaborn, pyarrow.
- **Parquet** :
  - Format de stockage efficace pour les données nettoyées et traitées.

---

### **Architecture du pipeline**

1. **Nettoyage et transformations des fichiers** :
   - Exécution : `python scripts/data_cleaning.py`.
   - Les fichiers CSV sont enregistrés dans `data/cleaned`.
   - Ils sont prêts pour une jointure.

2. **Ingestion des données** :
   - Producteur Kafka : `kafka/kafka_producer.py`.
   - Consommateur Kafka : `kafka/kafka_consumer.py`.
   - Nom du topic : `hospital-utilization`.

3. **Intégration des données** :
   - Script : `spark/hospital_streaming_pipeline.py`.
   - Les données en streaming sont jointes avec les datasets batch en utilisant les clés communes (`Date`, `Setting`).
   - Les résultats sont écrits dans `output/final_data` sous forme de fichiers Parquet.

4. **Analyse et traitement des données manquantes** :
   - Script d’analyse : `python scripts/analyze_missing_data.py`.
     - Génère un rapport sur les colonnes problématiques.
   - Script de traitement : `python scripts/handle_missing_data.py`.
     - Remplit les valeurs manquantes avec des valeurs par défaut.
     - Filtre les données invalides.
   - Résultat : fichiers sauvegardés dans `output/cleaned_data_processed`.

5. **Calcul des métriques** :
   - Métriques par date et globales :
     - `output/metrics/by_date` pour les métriques liées aux dates.
     - `output/metrics/overall` pour les agrégations globales.

6. **Visualisation des données** :
   - Script : `scripts/visualization_jupiter.ipynb`.
   - Génération de graphiques pour analyser les tendances et modèles.

---

### **Défis rencontrés**

- **Traitement en temps réel** :
  - Configuration de Kafka et intégration fluide avec Spark Streaming.
- **problème de configuration de Kafka (nombre de brokers limité à 1 sur Mac)**
  -Lors de la configuration de Kafka sur un Mac, on a constaté qu'il n'était pas possible d'ajouter plus d'un broker en raison des        limitations du système local (ressources, configuration réseau et système). Donc, On a pas pu configurer un facteur de réplication      supérieur à 1, ce qui est problématique pour la tolérance aux pannes.


---

### **Comment exécuter le projet**

#### **Pré-requis d'installation**

1. **Environnement Python**
   - Installer Python 3.8+.
   - Installer les bibliothèques nécessaires :
       
     pip install pandas numpy matplotlib seaborn kafka-python pyspark
      

2. **Configuration de Kafka**
   - Télécharger et installer Apache Kafka :
     - Démarrer le serveur Zookeeper :
         
       bin/zookeeper-server-start.sh config/zookeeper.properties
        
     - Démarrer le serveur Kafka :
         
       bin/kafka-server-start.sh config/server.properties
        

3. **Spark**
   - Installer Apache Spark (3.x recommandé).
   - Ajouter Spark à votre `PATH` ou configurez `SPARK_HOME`.

---

### **Étapes d'exécution**

1. **Nettoyage des données** :
     
   python scripts/data_cleaning.py
    

2. **Ingestion des données en streaming** :
     
   python kafka/producer/kafka_producer.py
   python kafka/producer/kafka_consumer.py
    

3. **Intégration des données avec Spark** :
     
   spark-submit spark/streaming/hospital_streaming_pipeline.py
    

4. **Analyse des données manquantes** :
     
   python scripts/analyze_missing_data.py
    

5. **Traitement des données manquantes et transformation** :
     
   python scripts/handle_missing_data.py
    

6. **Calcul des métriques** :

   python scripts/metrics_calculation.py
       
7. **Visualisation des données** :
     
   jupyter nbconvert --execute scripts/visualization_jupiter.ipynb


 
**Choix de la base de données**
- On a choisi **PostgreSQL** pour sa compatibilité avec les données relationnelles, sa gestion des séries temporelles via TimescaleDB, et sa fiabilité pour des analyses complexes.  
    
