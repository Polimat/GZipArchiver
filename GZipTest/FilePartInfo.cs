using System;

namespace GZipTest
{
    [Serializable]
    public class FilePartInfo
    {
        public FilePartInfo(long index, long offset, long size)
        {
            Index = index;
            Offset = offset;
            Size = size;
        }

        public long Index { get; private set; }
        public long Offset { get; private set; }
        public long Size { get; private set; }
    }
}
