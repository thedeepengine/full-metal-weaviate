from setuptools import setup, find_packages

setup(
    name='full-metal-weaviate',
    version='0.1',
    packages=['full_metal_weaviate'],
    description='full_metal_weaviate',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    author='Paul Hechinger',
    author_email='paul7junior@gmail.com',
    install_requires=['pyparsing>=3.0.0, <4.0.0', 
                      'returns>=0.23.0, <1.0.0',
                      'weaviate-client>=4.5.0, <5.0.0',
                      'rich>=13.0.0, <14.0.0',
                      'jmespath>=1.0.0, <2.0.0',
                      'pandas>=2.0.3, <3.0.0',
                      'bottleneck>=1.4.0',
                      'full-metal-monad @ git+https://github.com/thedeepengine/full-metal-monad.git'],
    extras_require={
        'dev': [
            'mock>=5.1.0',
            'sphinx>=7.4.7',
            'wheel',
            'twine'
        ]}
)
