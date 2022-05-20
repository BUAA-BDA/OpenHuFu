import setuptools

setuptools.setup(
    name="pyonedb",
    version="1.0.0"
    description="OneDB Python API",
    author="pxc",
    author_email="xc_pan@foxmail.com",
    packages=['pyonedb'],
    python_requires=">=3.7"
    install_requires=['jnius==1.4.1']
)