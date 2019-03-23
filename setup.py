from setuptools import setup, find_packages

setup(
    name="mongodata-distribution",
    version="0.1",
    install_requires=["matplotlib", "pymongo", "numpy"],
    # packages=['mongodata'], # mongodata not a package
    scripts=['mongodata-distribution.py'],
    author="xiang.gao",
    author_email="vegaoqiang@aliyun.com",
    description="generate a picture for mongo data distribution",
    classifiers=[
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6'
    ]
)