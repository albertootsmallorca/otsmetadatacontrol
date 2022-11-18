from setuptools import setup
print ("Creación del paquete otsmetadatacontrol")

#python setup.py bdist_egg

setup(name='otsmetadatacontrol',
        version= "1.1.1",
        description='Librería para gestionar las tablas de metadatos.',
        url='http://ots',
        author='Inetum',
        author_email='agarcia@nextdigital.es',
        license='MIT',
        packages=['otsmetadatacontrol'])

print ("Finalización de la creación del paquete execution_metadata")