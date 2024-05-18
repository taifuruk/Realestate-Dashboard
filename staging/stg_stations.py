#%%
import tempfile
import os
import requests
import zipfile
import geopandas as gpd

def main():
    # ZIPファイルのURL
    url = 'https://nlftp.mlit.go.jp/ksj/gml/data/N02/N02-22/N02-22_GML.zip'

    # 一時ディレクトリの作成
    with tempfile.TemporaryDirectory() as tmpdirname:
        # ZIPファイルのダウンロード
        response = requests.get(url)
        zip_path = os.path.join(tmpdirname, 'data.zip')

        # ZIPファイルを一時ディレクトリに保存
        with open(zip_path, 'wb') as f:
            f.write(response.content)

        # ZIPファイルを開き、特定のファイルを抽出
        with zipfile.ZipFile(zip_path, 'r') as z:
            # 'N02-22_Station.shp' と関連ファイルを確認
            shapefile_components = [f for f in z.namelist() if 'N02-22_Station' in f and f.endswith(('.shp', '.shx', '.dbf', '.prj'))]
            if shapefile_components:
                # 必要なシェープファイルのコンポーネントを一時ディレクトリに展開
                z.extractall(path=tmpdirname, members=shapefile_components)

                # geopandasでシェープファイルを読み込む
                shp_path = os.path.join(tmpdirname, 'N02-22_Station.shp')
                gdf = gpd.read_file(shp_path)

    return gdf