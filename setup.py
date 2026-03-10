from setuptools import find_packages, setup

with open("requirements.txt", encoding="utf-8") as f:
    install_requires = [line.strip() for line in f if line.strip() and not line.startswith("#")]

setup(
    name="erp_ai_assistant",
    version="0.1.0",
    description="Frappe app for ERP AI Assistant web and Desk experience",
    author="OpenAI",
    author_email="support@example.com",
    packages=find_packages(),
    zip_safe=False,
    include_package_data=True,
    install_requires=install_requires,
)
