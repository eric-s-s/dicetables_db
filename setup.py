# https://packaging.python.org/ for documentation

from setuptools import setup, find_packages


def readme():
    with open('README.rst') as f:
        return f.read()

setup(name='dicetables_db',
      version='1.1.1',
      description='a DiceTable db for a server',
      long_description=readme(),
      keywords='dice, die, statistics, table, probability, combinations',
      url='https://github.com/eric-s-s/dicetables_db',
      author='Eric Shaw',
      author_email='shaweric01@gmail.com',
      license='MIT',
      classifiers=[
        'Development Status :: 3 - Alpha',
        "Operating System :: OS Independent",
        'Intended Audience :: Developers',
        'Topic :: Scientific/Engineering :: Mathematics',
        'Topic :: Games/Entertainment :: Role-Playing',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
      ],
      packages=find_packages(exclude=['tests*', 'frontend*']),
      install_requires=['dicetables'],
      python_requires='>=3',
      include_package_data=True,
      zip_safe=False)
