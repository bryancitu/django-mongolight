from setuptools import setup, find_packages

setup(
    name="django-mongolight",
    version="0.1.13",
    author="Bryan Jesus Ramon Avila",
    author_email="bryanjesus.ra@gmail.com",
    description="A lightweight library for integrating Django with MongoDB.",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/bryancitu/django-mongolight",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
    install_requires=[
        "Django >= 4.0",
        "pymongo >= 4.0",
    ],
)
