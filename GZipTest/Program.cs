using System;
using System.Threading;

namespace GZipTest
{
    class Program
    {
        static int Main(string[] args)
        {
            if (args.Length < 3)
            {
                Console.WriteLine("Недостаточно параметров");
                ShowHelp();
                return 0;
            }
            bool compress;
            switch (args[0].ToUpper())
            {
                case "COMPRESS":
                    compress = true;
                    break;
                case "DECOMPRESS":
                    compress = false;
                    break;
                default:
                    ShowHelp();
                    return 0;
            }
            var archivator = new Archivator(args[1], args[2], compress);
            var thread = new Thread(archivator.Start);
            thread.Start();
            var readKeyThread = new Thread(ReadKey) {IsBackground = true};
            readKeyThread.Start();
            while (true)
            {
                lock (archivator.ResultLock)
                {
                    if (archivator.ErrorOccured || archivator.WorkCompleted) break;
                }
                if (_abort)
                {
                    archivator.Abort = true;
                    break;
                }
                
            }
            lock (archivator.ResultLock)
            {
                return archivator.WorkCompleted ? 1 : 0;
            }
        }


        private static bool _abort;

        private static void ReadKey()
        {
            while (true)
            {
                var key = Console.ReadKey(true);
                if ((key.Modifiers & ConsoleModifiers.Control) == 0) continue;
                if ((key.Key & ConsoleKey.C) == 0)
                    _abort = true;
            }
        }

        private static void ShowHelp()
        {
            Console.WriteLine("Параметры командной строки");
            Console.WriteLine("Разархивация: \"decompress [SourceFile] [DestinationFile]\"");
            Console.WriteLine("Архивация: \"compress [SourceFile] [DestinationFile]\"");
        }
    }
}
