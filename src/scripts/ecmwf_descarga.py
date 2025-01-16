import os
import cdsapi

class ERA5Downloader:
    '''
    Class to download ERA5 data from the CDS API
    # Siguiendo la guía https://cds.climate.copernicus.eu/how-to-api 
    '''
    def __init__(self, target_folder="../../data/raw/era5/", area=[13.0, -83.0, -4.6, -66.1]):
        self.target_folder = target_folder
        self.area = area
        self.client = cdsapi.Client()
        os.makedirs(self.target_folder, exist_ok=True)

    def download(self, years, variables, target_filename):
        dataset = "reanalysis-era5-single-levels"
        request = {
            "product_type": "reanalysis",
            "variable": variables,
            "year": years,
            "month": [f"{i:02d}" for i in range(1, 13)],
            "day": [f"{i:02d}" for i in range(1, 32)],
            "time": [f"{i:02d}:00" for i in range(24)],
            "format": "grib",
            "area": self.area
        }
        target_path = os.path.join(self.target_folder, target_filename)
        self.client.retrieve(dataset, request).download(target_path)
        print(f"Data downloaded to {target_path}")

    def download_temperature(self, years, target_filename):
        self.download(years, ["2m_temperature"], target_filename)

    def download_precipitation(self, years, target_filename):
        self.download(years, ["total_precipitation"], target_filename)

    def download_wind(self, years, target_filename):
        self.download(years, ["10m_u_component_of_wind", "10m_v_component_of_wind"], target_filename)

# Example usage
if __name__ == "__main__":
    downloader = ERA5Downloader()
    for year in range(1961, 2025):
        downloader.download_temperature([str(year)], f"era5_tmp_{year}.grib")
        downloader.download_precipitation([str(year)], f"era5_rain_{year}.grib")
        downloader.download_wind([str(year)], f"era5_wind_{year}.grib")
