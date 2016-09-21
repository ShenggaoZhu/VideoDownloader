@echo off

FOR /F "tokens=4 delims=," %%a IN (8.16-8.20_playback_part2.csv) DO (
	
	echo ">>>> now deal with %%a"
		
	for /F "tokens=1-8 delims=/" %%c in ("%%a") do (
	
		echo File-Name: %%i
REM	"ffmpeg/bin/ffmpeg" -y -i "%%d" -bsf:a aac_adtstoasc -vcodec copy -c copy -crf 50 "videos/%filename%.mp4"

		"ffmpeg/bin/ffmpeg" -y -i "%%a" -bsf:a aac_adtstoasc -vcodec copy -c copy -crf 50 "videos/%%i.mp4"

	)
	
)


ECHO.&PAUSE&GOTO:EOF
