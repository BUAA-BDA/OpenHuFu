import setuptools

with open('README.md', 'r') as f:
    readme = f.read()

setuptools.setup(
    name="pyonedb",
    version="1.0.0",
    description="OneDB Python API",
    author="pxc",
    author_email="xc_pan@foxmail.com",
    packages=['pyonedb'],
    python_requires=">=3.7",
    install_requires=['pyjnius==1.4.1'],
    test_requires=['pytest==7.1.2']
)