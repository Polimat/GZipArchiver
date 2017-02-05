using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace GZipTest
{
    
    public class FilePart
    {
        /// <summary>
        /// Архивированный кусок
        /// </summary>
        /// <param name="index">порядковый номер в последовательности частей</param>
        /// <param name="stream">поток с частью файла</param>
        /// <param name="blocks">Количество блоков памяти для дальнейших расчетов</param>
        public FilePart(long index, Stream stream, long blocks)
        {
            Index = index;
            Stream = stream;
            try
            {
                Stream.Position = 0;
            }
            catch (NotSupportedException)
            {
                
            }            
            Blocks = blocks;
        }

        public long Index { get; private set; }
        public Stream Stream { get; private set; }
        public long Blocks { get; private set; }
    }
}
