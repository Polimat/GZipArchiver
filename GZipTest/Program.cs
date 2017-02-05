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
            var archivator = new Archivator(args[1], args[2]);
            Thread thread = null;
            switch (args[0].ToUpper())
            {
                case "COMPRESS":
                    thread = new Thread(archivator.Compress);
                    thread.Start();
                    break;
                case "DECOMPRESS":
                    thread = new Thread(archivator.Decompress);
                    thread.Start();
                    break;
                default:
                    ShowHelp();
                    return 0;
            }
            while (true)
            {
                lock (archivator.ResultLock)
                {
                    if (archivator.ErrorOccured) return 0;
                    if (archivator.WorkCompleted) return 1;
                }
                var key = Console.ReadKey(true);
                if ((key.Modifiers & ConsoleModifiers.Control) == 0) continue;
                if ((key.Key & ConsoleKey.C) == 0) continue;
                archivator.Abort = true;
                break;
            }
            return archivator.WorkCompleted ? 1 : 0;
        }

        private static void ShowHelp()
        {
            Console.WriteLine("Параметры командной строки");
            Console.WriteLine("Разархивация: \"decompress [SourceFile] [DestinationFile]\"");
            Console.WriteLine("Архивация: \"compress [SourceFile] [DestinationFile]\"");
        }
    }
}
