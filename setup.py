from setuptools import setup, find_packages

setup(
    name="datarush",
    version="0.1.0",
    author="Vadim Titov",
    author_email="titov.hse@gmail.com",
    description="Process data when you are in a rush",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/your-repo",
    packages=find_packages(),
    install_requires=[
        "pandas>=1.3.0",
        "pydantic>=2",
        "streamlit",
        "boto3>=1.37.4,<2",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.12",
    entry_points={
        "console_scripts": [
            "datarush = datarush.runner:run_template_from_command_line",
        ],
    },
)
