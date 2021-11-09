#https://blog.csdn.net/qq_31010925/article/details/110308465
sudo apt install xvfb
wget -c https://launchpad.net/~canonical-chromium-builds/+archive/ubuntu/stage/+files/chromium-chromedriver_95.0.4638.69-0ubuntu0.18.04.1_arm64.deb
wget -c https://launchpad.net/~canonical-chromium-builds/+archive/ubuntu/stage/+files/chromium-browser_95.0.4638.69-0ubuntu0.18.04.1_arm64.deb
wget -c https://launchpad.net/~canonical-chromium-builds/+archive/ubuntu/stage/+files/chromium-codecs-ffmpeg-extra_95.0.4638.69-0ubuntu0.18.04.1_arm64.deb
wget -c https://launchpad.net/~canonical-chromium-builds/+archive/ubuntu/stage/+files/chromium-codecs-ffmpeg_95.0.4638.69-0ubuntu0.18.04.1_arm64.deb
sudo dpkg -i chromium-codecs-ffmpeg-extra_95.0.4638.69-0ubuntu0.18.04.1_arm64.deb
sudo dpkg -i chromium-codecs-ffmpeg_95.0.4638.69-0ubuntu0.18.04.1_arm64.deb
sudo dpkg -i chromium-browser_95.0.4638.69-0ubuntu0.18.04.1_arm64.deb
sudo dpkg -i chromium-chromedriver_95.0.4638.69-0ubuntu0.18.04.1_arm64.deb
