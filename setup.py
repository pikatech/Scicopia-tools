import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="scicopia-tools",
    version="0.5.2",
    author="Pikatech",
    author_email="pikatech@mail.de",
    description="Document analysis modules for Scicopia.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/pikatech/Scicopia-tools",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Development Status :: 3 - Alpha",
        "Framework :: Flask",
    ],
    python_requires='>=3.6',
)
