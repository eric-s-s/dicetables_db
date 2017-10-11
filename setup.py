from setuptools import setup


def readme():
    with open('README.md') as f:
        return f.read()

setup(name='dicetables_db',
      version='1.0.0',
      description='a DiceTable db for a server',
      long_description=readme(),
      keywords='dice, die, statistics, table, probability, combinations',
      url='http://github.com/eric-s-s/dice-tables',  # TODO
      author='Eric Shaw',
      author_email='shaweric01@gmail.com',
      license='MIT',
      classifiers=[  # TODO
        'Development Status :: 4 - Beta',
        "Operating System :: OS Independent",
        'Intended Audience :: Developers',
        'Topic :: Scientific/Engineering :: Mathematics',
        'Topic :: Games/Entertainment :: Role-Playing',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
      ],
      packages=['dicetables', 'dicetables.tools', 'dicetables.factory', 'dicetables.eventsbases'],  # TODO
      install_requires=[],  # TODO
      test_suite='nose.collector',  # TODO
      tests_require=['nose'],  # TODO
      include_package_data=True,
      zip_safe=False)

# TODO a script for setting up test suite and executing bash script for setting up mondodb.
