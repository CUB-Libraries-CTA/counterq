from setuptools import setup, find_packages

setup(name='counterq',
      version='0.0',
      packages= find_packages(),
      install_requires=[
            'sqlalchemy==1.4.31',
            'pandas',
            'boto3',
            'XlsxWriter==3.0.1',
            'mysqlclient',
            ],
)