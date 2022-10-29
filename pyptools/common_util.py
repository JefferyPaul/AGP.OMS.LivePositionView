import os


def readlines_reverse(p):
    """
    从末端开始，逐行读取文件。
    跳过 \r\n 等换行符；
    :param p:
    :return:
    """
    with open(p, ) as f:
        f.seek(0, os.SEEK_END)
        position = f.tell()
        line = ''
        while position >= 0:
            f.seek(position)
            next_char = f.read(1)
            position -= 1
            if next_char.strip() == '':
                if len(line) > 0:
                    yield line[::-1].strip()
                    line = ''
            else:
                line += next_char
        yield line[::-1].strip()


def read_last_line(p) -> str:
    """
    读取大文件的最后一行，非空行
    :param p:
    :return:
    """
    with open(p, 'rb') as f:
        off = -500
        while True:
            f.seek(off, 2)  # seek(off, 2)表示文件指针：从文件末尾(2)开始向前50个字符(-50)
            lines = f.readlines()  # 读取文件指针范围内所有行
            lines = [_.strip() for _ in lines if _.strip()]  # 跳过空行
            if len(lines) >= 2:  # 判断是否最后至少有两行，这样保证了最后一行是完整的
                last_line = lines[-1]  # 取最后一行
                break
            off *= 2
        return last_line.decode('utf-8')


# if __name__ == '__main__':
#     p = r'C:\Users\Jeffery\Desktop\RawSignals.csv'
#     for n, line in enumerate(readlines_reverse(p)):
#         print(n, line)
#         if n == 4:
#             break
