# Self-Describing Media Archives

This web-based application turns a simple media archive on an object storage into a one capable of returning queries based on media properties and content.  
  - i.e., “Get me the media, which was created in between ‘these dates’, in ‘this neighborhood’, on ‘this street’, at an altitude of at least Hm from sea level, which contains ‘these objects’.”

- Target use cases:
   -  Media & Entertainment: Smart archives
   -  Forensic analyses
   -  Labeling media data for AI/ML data sets for training

### Workflow

![workflow](/site/static/pic/workflow.png)

### Some Screenshots From the App

> Source settings
>
> ![source-settings](/site/static/pic/source-settings.png)

> Image processing settings
>
> ![iamge-processing-settings](/site/static/pic/image-processing-settings.png)  

> Runtime settings
>
> ![runtime settings](/site/static/pic/runtime-settings.png)  

### Requirements
- S3 compatible object storage system
- Media files conforming to Exchangeable Image File Format (EXIF) format, with Exif tags turned on
- Computer Vision AI to label the objects in a media file (optional)
- An Iceberg catalog on a Starburst Galaxy/Enterprise or DDLH instance (optional)
- Windows Server 2022, Windows 11 (might work with other Windows flavours and versions but untested)
- Python for Windows version 3.11 - 3.12.6 (might work with earlier versions but untested)

### Installation

1. Install [Python for Windows](https://www.python.org/downloads/release/python-3126/)
   - Select "Add Python to environment variables" in Advanced Options
   - Select "Disable Windows path limit" in the last step
2. Install [Git for Windows](https://git-scm.com/download/win)  
3. Switch into desired drive (i.e., "E"), create "programs" folder and clone this repository under "programs" directory
```console
E:  
mkdir programs  
cd programs  
git clone https://github.com/baris-saltik/self-describing-media-archives  
```
4. Switch into self-describing-media-archives directory, create a virtual Python environment named ".venv" and activate that environment.
```console
cd self-describing-media-archives  
python -m venv .venv  
.venv\Scripts\activate.bat  
```
5. Install required Python packages
```console
python -m pip install -r Requirements.txt
```
6. Launch the application
   - For web version, run the following:
   ```console
   python sdma_web.py
   ```
   -  For cli version, tun the following:
      -  Edit the "/config/main.yaml" configuration file
      -  Run the following:
   ```console
   python sdma_main.py
   ```
7. For web version open up a web browser and direct it to the following address.
```console
https://127.0.0.1:5000
```