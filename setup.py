from setuptools import setup

setup(
    name = 'little_pger',
    version = '1.0.2',
    author = 'Christian Jauvin',
    author_email = 'cjauvin@gmail.com',
    description = (
        "A thin layer just a tad above SQL, for use with PostgreSQL and psycopg2, when you want to wrap queries "
        "in a convenient way, using plain data structures (but you don't feel like using an ORM, for some reason)."),
    license = 'BSD',
    keywords = 'postgresql psycopg2 sql',
    url = 'https://github.com/cjauvin/little_pger',
    #long_description=open('README.md').read(),
    py_modules=['little_pger'],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Utilities'
    ],
    install_requires = ['psycopg2', 'six']
)
