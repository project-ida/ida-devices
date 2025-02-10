from setuptools import setup, find_packages

# Read dependencies from requirements.txt
def read_requirements():
    with open("requirements.txt") as f:
        return f.read().splitlines()

setup(
    name="ida-devices",
    version="1.0.0",
    packages=find_packages(),
    install_requires=read_requirements(),  # Uses requirements.txt
)
