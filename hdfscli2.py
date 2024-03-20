import httpx
import os
import sys
from typing import List


class HDFSClient:
    def __init__(self, host: str, port: int, username: str) -> None:
        self.base_url = f"http://{host}:{port}/webhdfs/v1"
        self.username = username
        self.current_dir = "/"

    def mkdir(self, dir_name: str) -> None:
        """Create a directory in HDFS."""
        url = f"{self.base_url}{self.current_dir}{dir_name}?user.name={self.username}&op=MKDIRS"
        response = httpx.put(url)
        if response.status_code == 200:
            print(f"Directory {dir_name} created successfully.")
        else:
            print(f"Error creating directory {dir_name}: {response.json()}")

    def put(self, local_file_path: str) -> None:
        """Upload a local file to HDFS."""
        filename = os.path.basename(local_file_path)
    # Инициирующий запрос для создания файла
        initiate_url = f"{self.base_url}{self.current_dir}{filename}?user.name={self.username}&op=CREATE&overwrite=true"
        initiate_response = httpx.put(initiate_url, follow_redirects=False)
    
        if initiate_response.status_code == 307:  # Temporary Redirect
        # Получение URL из заголовка Location для загрузки файла
            upload_url = initiate_response.headers['Location']
            with open(local_file_path, 'rb') as local_file:
            # Выполнение запроса на загрузку файла
                upload_response = httpx.put(upload_url, content=local_file.read())
                if upload_response.status_code in [200, 201]:  # Успешная загрузка
                    print(f"File {filename} uploaded successfully.")
                else:
                    try:
                        error_msg = upload_response.json()
                    except ValueError:  # handling cases where the response is not JSON
                        error_msg = upload_response.text
                    print(f"Error uploading file {filename}: {error_msg}")
        else:
            try:
                error_msg = initiate_response.json()
            except ValueError:  # Если ответ не в формате JSON
                error_msg = initiate_response.text
            print(f"Error initiating file upload {filename}: {error_msg}")

    def get(self, hdfs_file_name: str, local_dest_path: str) -> None:
        """Download a file from HDFS."""
    # Инициирующий запрос для получения URL скачивания
        initiate_url = f"{self.base_url}{self.current_dir}{hdfs_file_name}?user.name={self.username}&op=OPEN"
        initiate_response = httpx.get(initiate_url, follow_redirects=False)
    
        if initiate_response.status_code == 307:  # Temporary Redirect
        # Получение URL из заголовка Location для скачивания файла
            download_url = initiate_response.headers['Location']
            download_response = httpx.get(download_url)
        
            if download_response.status_code == 200:
                with open(local_dest_path, 'wb') as local_file:
                    local_file.write(download_response.content)
                print(f"File {hdfs_file_name} downloaded successfully.")
            else:
                try:
                    error_msg = download_response.json()
                except ValueError:  # handling cases where the response is not JSON
                    error_msg = download_response.text
                print(f"Error downloading file {hdfs_file_name}: {error_msg}")
        elif initiate_response.status_code == 200:
        # Если файл небольшой и сервер не требует перенаправления
            with open(local_dest_path, 'wb') as local_file:
                local_file.write(initiate_response.content)
            print(f"File {hdfs_file_name} downloaded successfully.")
        else:
            try:
                error_msg = initiate_response.json()
            except ValueError:  # Если ответ не в формате JSON
                error_msg = initiate_response.text
            print(f"Error initiating file download {hdfs_file_name}: {error_msg}")

    def delete(self, hdfs_file_name: str) -> None:
        """Delete a file or directory from HDFS."""
        url = f"{self.base_url}{self.current_dir}{hdfs_file_name}?user.name={self.username}&op=DELETE"
        response = httpx.delete(url)
        if response.status_code == 200:
            print(f"File or directory {hdfs_file_name} deleted successfully.")
        else:
            print(f"Error deleting file or directory {hdfs_file_name}: {response.json()}")

    def ls(self) -> List[str]:
        """List the contents of the current directory in HDFS."""
        url = f"{self.base_url}{self.current_dir}?user.name={self.username}&op=LISTSTATUS"
        response = httpx.get(url)
        if response.status_code == 200:
            file_statuses = response.json()['FileStatuses']['FileStatus']
            return [file_status['pathSuffix'] for file_status in file_statuses]
        else:
            print(f"Error listing directory {self.current_dir}: {response.json()}")
            return []

    def cd(self, dir_name: str) -> None:
        """Change the current directory in HDFS."""
        if dir_name == "..":
            self.current_dir = os.path.dirname(self.current_dir)
        else:
            self.current_dir = os.path.join(self.current_dir, dir_name)

    def append(self, local_file_path: str, hdfs_file_name: str) -> None:
        """Append a local file to a file in HDFS."""
    # Шаг 1: Инициируем запрос APPEND для получения URI
        initiate_url = f"{self.base_url}{self.current_dir}{hdfs_file_name}?user.name={self.username}&op=APPEND"
        initiate_response = httpx.post(initiate_url, follow_redirects=False)
    
        if initiate_response.status_code == 307:  # Temporary Redirect
        # Шаг 2: Получаем Location из заголовков для отправки данных
            append_url = initiate_response.headers['Location']
            with open(local_file_path, 'rb') as local_file:
                data = local_file.read()
                append_response = httpx.post(append_url, content=data)
            
                if append_response.status_code in [200, 201]:  # Успешное добавление
                    print(f"File {local_file_path} appended successfully to {hdfs_file_name}.")
                else:
                    # Обработка ошибок при добавлении данных
                    try:
                        error_msg = append_response.json()
                    except ValueError:  # Если ответ не JSON
                        error_msg = append_response.text
                    print(f"Error appending file {local_file_path} to {hdfs_file_name}: {error_msg}")
        else:
            # Обработка ошибок инициации операции APPEND
            try:
                error_msg = initiate_response.json()
            except ValueError:
                error_msg = initiate_response.text
            print(f"Error initiating append for {local_file_path} to {hdfs_file_name}: {error_msg}")

    def lls(self, local_dir: str = '.') -> List[str]:
        """List the contents of the current local directory."""
        try:
            files_and_dirs = os.listdir(local_dir)
            return files_and_dirs
        except Exception as e:
            print(f"Error listing local directory {local_dir}: {e}")
            return []

    def lcd(self, local_dir: str) -> None:
       """Change the current local directory."""
       try:
           # Проверяем, существует ли директория
           if not os.path.exists(local_dir):
               # Если директории нет, создаём её
               os.makedirs(local_dir)
               print(f"Directory {local_dir} created.")
               
           # Меняем текущую директорию
           os.chdir(local_dir)
           print(f"Changed local directory to {local_dir}")
       except Exception as e:
           print(f"Error changing local directory to {local_dir}: {e}")


if __name__ == "__main__":
    host = sys.argv[1]
    port = int(sys.argv[2])
    username = sys.argv[3]

    client = HDFSClient(host, port, username)

    # Пример использования
    #client.mkdir("test_dir")
    #client.put("local_file.txt")
    #client.put("hdfs_file.txt")
    #client.get("hdfs_file.txt", "local_dest.txt")
    #files = client.ls()
    #print("Files:", files)
    #client.cd("..")
    #client.delete("test_dir")
    #client.put("local_file_to_append.txt")
    #client.append("local_file_to_append.txt", "hdfs_file.txt")
    #local_files = client.lls()
    #print("Local files:", local_files)
    #client.lcd("/new/local/directory")
    client.put("products.csv")