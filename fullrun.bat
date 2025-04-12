@echo off
REM === Configuration ===
REM Set the path to your Java 21 installation (if not on system PATH)
set JAVA_HOME="C:\Program Files\Java\jdk-21"
REM Set the path to your JavaFX SDK libraries
set JAVAFX_SDK_LIB="lib\javafx\lib"
REM Set the name of your application JAR
set APP_JAR="untitled.jar"

REM === JVM Arguments for JavaFX Modules ===
REM Specifies where to find the JavaFX modules
set JAVAFX_MODULE_PATH=--module-path %JAVAFX_SDK_LIB%
REM Specifies which JavaFX modules your application needs
set JAVAFX_ADD_MODULES=--add-modules javafx.controls,javafx.fxml,javafx.graphics,javafx.media 
REM ^^^ Added javafx.media as the error originates there ^^^

REM === JVM Arguments to Fix IllegalAccessError (JPMS) ===
REM Allows internal JavaFX classes to be accessed by code in the 'unnamed module' (your fat JAR)
set JPMS_EXPORTS=^
 --add-exports javafx.base/com.sun.javafx=ALL-UNNAMED ^
 --add-exports javafx.graphics/com.sun.javafx.application=ALL-UNNAMED ^
 --add-exports javafx.graphics/com.sun.glass.ui=ALL-UNNAMED ^
 --add-exports javafx.graphics/com.sun.javafx.tk=ALL-UNNAMED ^
 --add-exports javafx.media/com.sun.media.jfxmedia.locator=ALL-UNNAMED

REM Note: Removed -Djava.library.path because JavaCV natives should now be loaded
REM       automatically from within the fat JAR after extraction. Setting an
REM       external path is usually not needed and can cause conflicts.

REM === Run the Application ===
echo Starting application...
%JAVA_HOME%\bin\java %JAVAFX_MODULE_PATH% %JAVAFX_ADD_MODULES% %JPMS_EXPORTS% -jar %APP_JAR% %*

echo Application finished.