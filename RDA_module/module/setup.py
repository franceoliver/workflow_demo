from setuptools import find_packages
from distutils.core import setup


# here = os.path.abspath(os.path.dirname(__file__))
#
# with open(os.path.join(here, 'README.md'), 'r', encoding='utf-8') as f:
#     long_description = f.read()
#
# with open(os.path.join(here, 'LICENSE.md'), 'r', encoding='utf-8') as f:
#     license = f.read()



metadata = {
    'name'              : 'wage_trust_rda',
    'version'           : '0.0.1',
    'author'            : ['Oliver France.',  'Xun Yang'],
    'author_email'      : ['oliver.b.france@pwc.com', 'xun.yang@pwc.com'],
    'maintainer'        : 'Xun Yang',
    'maintainer_email'  : 'xun.yang@pwc.com',
    'url'               : 'https://github.pwc.com/wage-trust/workflow_demo',
    'description'       : 'wage-trust-RDA',
    'classifiers'       : [ 'Development Status :: 3 - Alpha',
                            'Programming Language :: Python',
                            'Programming Language :: Python :: 3',
                            'Programming Language :: Python :: 3.6',
                            'Intended Audience :: Data/Analytics',
                            'Topic :: wage trust',
                            'Operating System :: OS Independent'],
    'install_requires'  : [ 'numpy>=1.11',
                            'pandas>=0.23',
                            'psycopg2>=2.7'],
    'packages'          : find_packages(),
    'package_dir'       : {'wage_trust_rda' : 'wage_trust_rda'},
}

setup (**metadata)
