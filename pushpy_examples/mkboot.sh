#!/bin/bash
python3 -m zipapp boot -m 'ex_boot:main'
unzip -t boot.pyz
