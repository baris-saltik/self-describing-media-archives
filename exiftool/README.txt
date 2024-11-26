
ExifTool package for 64-bit Windows
___________________________________

Double-click on "exiftool(-k).exe" to read the application documentation, or
drag-and-drop files to extract metadata.

For command-line use, rename to "exiftool.exe".

Run directly in from the exiftool-##.##_64 folder, or copy the .exe and the
"exiftool_files" folder to wherever you want (preferably somewhere in your
PATH) to run from there.

See https://exiftool.org/install.html for more installation instructions.

Geolocation Bearing: Bearing is calculated as an angle measured in degrees in a clockwise direction from true north


Examples
___________________________________
exiftool -api geolocation -json "C:\Users\saltib\OneDrive - Dell Technologies\Documents\dokumanlar\resimler\sb\test_data\*"

exiftool -api geolocation -filename -filesize -mimetype -orientation -imagesize -make -model -software -createdate -flash -GPSAltitude -GPSDateTime -GPSLatitude -GPSLongitude -GeolocationCity -GeolocationRegion -GeolocationCountryCode -GeolocationCountry -GeolocationTimeZone -GeolocationPopulation -GeolocationPosition -GeolocationDistance -GeolocationBearing -json "C:\Users\saltib\OneDrive - Dell Technologies\Documents\dokumanlar\resimler\sb\test_data\*"

exiftool -api geolocation -MyTags -json "C:\Users\saltib\OneDrive - Dell Technologies\Documents\dokumanlar\resimler\sb\test_data\*"


.ExifTool_config
____________________________________
$Image::ExifTool::Geolocation::geoDir = 'C:\Users\saltib\bilgi\indir\exiftool\Geolocation500';

%Image::ExifTool::UserDefined::Shortcuts = (
    MyTags => ['filesize','mimetype','orientation','imagesize','make','model','software','createdate','flash','GPSAltitude','GPSDateTime','GPSLatitude','GPSLongitude','GeolocationCity','GeolocationRegion','GeolocationCountryCode','GeolocationCountry','GeolocationTimeZone','GeolocationPopulation','GeolocationPosition','GeolocationDistance','GeolocationBearing']

);