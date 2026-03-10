from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.os_manager import ChromeType
from selenium.webdriver.chrome.service import Service
from selenium import webdriver
import platform 
import zipfile
import shutil
import os

class CBrowser():
    """
    # CLASSE PARA CONTROLE DO NAVEGADOR CHROMIUM
    
    Após instanciar e extrair o zip com sucesso, execute OpenBrowser para abrir o navegador.
    """
    
    def __init__(self) -> None:
        self.BaseUrl = "https://www.google.com.br"
        self.driver = None
        self.timeout_load = 60
        self.system = platform.system()
        if self.system == "Windows":
            self.path_browserdriver = os.path.join("Modulos", "BROWSER", "chrome-win")
            self.CleanChrome()
            self.ExtractZip()
        
    def CleanChrome(self) -> bool:
        if os.path.exists(self.path_browserdriver):
            try:
                shutil.rmtree(self.path_browserdriver)
            except Exception as e:
                print(f"NÃO FOI POSSÍVEL DELETAR O BROWSER ANTIGO - {e}")
        return not os.path.exists(self.path_browserdriver)
    
    def ExtractZip(self) -> str:
        zip_path = f"{self.path_browserdriver}.zip"
        extract_to = self.path_browserdriver.replace(os.path.basename(self.path_browserdriver), "")
        if not os.path.exists(extract_to):
            os.makedirs(extract_to)
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
        return os.path.exists(self.path_browserdriver)
    
    def OpenBrowser(self):
        # Paths
        if self.system == "Windows":
            path_browser_exe = os.path.join(os.getcwd(), self.path_browserdriver, "chrome.exe")
            path_driver = os.path.join(os.getcwd(), self.path_browserdriver, "chromedriver.exe")

        # Configurando opções do Chrome
        chrome_options = webdriver.ChromeOptions()  # Options()  webdriver.ChromeOptions()
        chrome_options.add_argument("--verbose")
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument("--window-size=1920, 1080")
        chrome_options.add_argument("--headless") # Executa o Chrome em modo headless (sem interface gráfica)
        chrome_options.add_argument("--no-sandbox") # Desabilita o sandbox para evitar problemas de permissões
        chrome_options.add_argument("--disable-dev-shm-usage") # Desabilita o uso de /dev/shm para evitar problemas de memória compartilhada
        chrome_options.add_argument('--disable-extensions')
        chrome_options.add_argument('--disable-software-rasterizer')
        # chrome_options.add_argument('--remote-debugging-port=9222')
        
        if self.system == "Windows":
            chrome_options.binary_location = path_browser_exe
            service = Service(executable_path=path_driver)
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
        elif self.system == "Linux":
            driver_path = ChromeDriverManager(chrome_type=ChromeType.CHROMIUM).install()
            service = Service(executable_path=driver_path)
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
        elif self.system == "Darwin":  # macOS
            driver_path = ChromeDriverManager().install()
            service = Service(executable_path=driver_path)
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
        else:
            raise RuntimeError(f"Sistema operacional '{self.system}' não suportado pelo CBrowser.")

        self.driver.set_page_load_timeout(self.timeout_load * 5)
        self.driver.implicitly_wait(self.timeout_load)
        self.driver.get(self.BaseUrl)
        
        return self.driver

    