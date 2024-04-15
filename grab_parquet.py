from minio import Minio
import urllib.request
import pandas as pd
import sys

def main():
    grab_data()
    write_data_minio()

def grab_data() -> None:
    """Grab the data from New York Yellow Taxi

    This method download x files of the New York Yellow Taxi. 

   Files need to be saved into "../../data/raw" folder
   This methods takes no arguments and returns nothing.
    """
    url1 = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-11.parquet"
    destination1 = "C:\\Users\\OUMOU THIAM\\ATL-Datamart\\data\\raw\\novembre.parquet"

    #url2 = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-12.parquet"
    #destination2 = "C:\\Users\\OUMOU THIAM\\ATL-Datamart\\data\\raw\\decembre.parquet"

    # Téléchargement du fichier depuis l'URL
    #urllib.request.urlretrieve(url1, destination1)
    #urllib.request.urlretrieve(url2, destination2)
    #print(f"Les fichiers ont été téléchargés avec succès")

def write_data_minio():

    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )

   # bucket: str = "OUMISHOU"
    #found = client.bucket_exists(bucket)
    #if not found:
     #   client.make_bucket(bucket)
    #else:
     #   print("Bucket " + bucket + " existe déjà")

    bucket = "oumishou"

    # Liste des fichiers à télécharger
    fichiers_locaux = ["C:\\Users\\OUMOU THIAM\\ATL-Datamart\\data\\raw\\novembre.parquet",
                       "C:\\Users\\OUMOU THIAM\\ATL-Datamart\\data\\raw\\decembre.parquet"]

    for fichier_local in fichiers_locaux:
        # Extraire le nom de fichier du chemin local
        nom_fichier = fichier_local.split("\\")[-1]

        # Chemin dans MinIO (dans le bucket)
        chemin_minio = nom_fichier

        # Vérifier si le bucket existe, le créer sinon
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)

        try:
            # Téléverser le fichier dans MinIO
            client.fput_object(bucket, chemin_minio, fichier_local)

            print(f"Les fichiers {fichier_local} ont été téléversés avec succès dans MinIO.")
        except Exception as e:
            print(f"Erreur lors du téléversement des fichiers {fichier_local} dans MinIO : {e}")


if __name__ == '__main__':
    sys.exit(main())
