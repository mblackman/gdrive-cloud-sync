from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
import google.auth
import io
import logging
import os
import tarfile
import datetime

SCOPES = ['https://www.googleapis.com/auth/drive']

def backup_folder(drive_service, source_folder_ids, dest_folder_id, backup_name):
    """
    Backs up a Google Drive folder.

    Args:
        drive_service: The Drive API service instance.
        source_folder_ids: The IDs of the folders to back up.
        dest_folder_id: The ID of the folder to store backups in.
        backup_name: The name of the backup file to create.
    """
    logging.info("Starting backup process...")
    logging.info(f"Source folder IDs: {source_folder_ids}")
    logging.info(f"Destination folder ID: {dest_folder_id}")
    logging.info(f"Backup name: {backup_name}")

    now = datetime.datetime.now()
    backup_file_name = now.strftime('%Y-%m-%d_%H-%M-%S-%f')[:-3] + "_" + backup_name + ".tar.gz"

    archive_file_path = '/tmp/backup.tar.gz'
    with tarfile.open(archive_file_path, 'w:gz', compresslevel=9) as tar:
        for source_folder_id in source_folder_ids:
            add_files_to_archive(tar, drive_service, source_folder_id, '')

    file_metadata = {
        'name': backup_file_name,
        'parents': [dest_folder_id]
    }
    media = MediaFileUpload(archive_file_path, mimetype='application/gzip')
    drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    os.remove(archive_file_path)
    logging.info(f"Created backup archive: {backup_file_name}")

def add_files_to_archive(tar, drive_service, folder_id, current_path):
    """
    Recursively adds files and folders to the tar.gz archive.
    """
    results = drive_service.files().list(
        q=f"'{folder_id}' in parents and trashed = false",
        fields="nextPageToken, files(id, name, mimeType)"
    ).execute()
    items = results.get('files', [])

    for item in items:
        item_path = os.path.join(current_path, item['name'])
        if item['mimeType'] == 'application/vnd.google-apps.folder':
            add_files_to_archive(tar, drive_service, item['id'], item_path)
        else:
            request = drive_service.files().get_media(fileId=item['id'])
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()

            fh.seek(0)
            tarinfo = tarfile.TarInfo(name=item_path)
            tarinfo.size = fh.getbuffer().nbytes
            tar.addfile(tarinfo, fh)

def delete_old_backups(drive_service, dest_folder_id, versions_to_keep, backup_name):
    """
    Deletes old backup files, keeping only the specified number of versions.
    """
    logging.info("Deleting old backups...")

    results = drive_service.files().list(
        q=f"'{dest_folder_id}' in parents and mimeType='application/zip' and name contains '{backup_name}'",
        orderBy='createdTime desc',
        fields="nextPageToken, files(id, name)"
    ).execute()
    files = results.get('files', [])

    if len(files) > versions_to_keep:
        files_to_delete = files[versions_to_keep:]
        logging.info(f"Deleted old backup files: {', '.join([file['name'] for file in files_to_delete])}")
        for file in files_to_delete:
            drive_service.files().delete(fileId=file['id']).execute()

def load_env_vars():
    """
    Loads environment variables from .env file for local development.
    """
    if os.environ.get('ENVIRONMENT') == 'LOCAL':
        from dotenv import load_dotenv
        load_dotenv()

def main(request):
    """
    Cloud Function entry point.

    Args:
        request: The HTTP request object.
    """
    load_env_vars()

    source_folder_ids = os.environ.get('SOURCE_FOLDER_IDS').split(',')
    dest_folder_id = os.environ.get('DEST_FOLDER_ID')
    backup_name = os.environ.get('BACKUP_NAME')
    versions_to_keep = int(os.environ.get('VERSIONS_TO_KEEP', 5))

    credentials, project_id = google.auth.default(scopes=SCOPES)
    drive_service = build('drive', 'v3', credentials=credentials)

    backup_folder(drive_service, source_folder_ids, dest_folder_id, backup_name)
    delete_old_backups(drive_service, dest_folder_id, versions_to_keep, backup_name)

    logging.info("Backup completed successfully!")
    return 'Backup completed successfully!'

if __name__ == '__main__':
    main(None)