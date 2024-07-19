import gzip
import re

import os
import rcssmin as minicss
import rjsmin as minijs

staticfolder = "static"
whitecompress = False


def static_compress(option):
    print(option)
    dcompress = False
    compress = False
    if option == "compress":
        dcompress = False
        compress = True
    elif option == "recompress":
        dcompress = True
        compress = True
    elif option == "dcompress":
        dcompress = True
        compress = False

    if "/" not in staticfolder:
        rootDir = "./{}".format(staticfolder)
    else:
        rootDir = staticfolder

    SKIP_COMPRESS_EXTENSIONS = (
        # Images
        "jpg",
        "jpeg",
        "png",
        "gif",
        "webp",
        # Compressed files
        "zip",
        "gz",
        "tgz",
        "bz2",
        "tbz",
        # Flash
        "swf",
        "flv",
        # Fonts
        "woff",
        "woff2",
    )

    skipfile = re.compile(
        r"\.({0})$".format("|".join(map(re.escape, SKIP_COMPRESS_EXTENSIONS))),
        re.IGNORECASE,
    )

    def checkext(fname):
        return not skipfile.search(fname)

    for dirName, subdirList, fileList in os.walk(rootDir, topdown=False):
        for fname in fileList:
            if checkext(fname):
                newfname = str(fname)
                if fname.endswith(".css") and not fname.endswith(".min.css"):
                    with open(
                        "".join((dirName, "/", fname)), "r", encoding="utf-8"
                    ) as f:
                        data = minicss.cssmin(f.read())
                    newfname = fname.replace(".css", ".min.css")
                    with open(
                        "".join((dirName, "/", newfname)), "w", encoding="utf-8"
                    ) as fw:
                        fw.write(data)
                elif fname.endswith(".js") and not fname.endswith(".min.js"):
                    with open(
                        "".join((dirName, "/", fname)), "r", encoding="utf-8"
                    ) as f:
                        data = minijs.jsmin(f.read())
                    newfname = fname.replace(".js", ".min.js")
                    with open(
                        "".join((dirName, "/", newfname)), "w", encoding="utf-8"
                    ) as fw:
                        fw.write(data)
                fullpath = dirName + "/" + newfname
                gzippath = fullpath + ".gz"
                gzipfile = newfname + ".gz"
                if newfname[0] == ".":
                    try:
                        os.remove(fullpath)
                    except Exception as exc:
                        print(exc)
                    continue
                if dcompress and gzipfile in fileList:
                    os.remove(gzippath)
                if compress and (gzipfile not in fileList or dcompress):
                    originalfile = open(fullpath, "rb")
                    compressedfile = gzip.open(gzippath, "wb")
                    compressedfile.writelines(originalfile)
                    compressedfile.close()
                    originalfile.close()
    return


if __name__ == "__main__":
    static_compress("recompress")
