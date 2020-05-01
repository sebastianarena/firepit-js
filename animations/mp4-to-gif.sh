#!/bin/bash
rm -rf palette.png
rm -rf $1.gif
# SIZE="1212:448"
SIZE="800:-1"
FILTERS="fps=15,scale=$SIZE:flags=lanczos"
ffmpeg -i $1.mp4 -vf $FILTERS,palettegen palette.png
ffmpeg -i $1.mp4 -i palette.png -filter_complex "$FILTERS[x];[x][1:v]paletteuse" $1.gif
rm -rf palette.png
