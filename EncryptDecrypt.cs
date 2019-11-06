using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace WebApplication1.DataManager
{

    // Custom Encrype Decrypt Class
    public class EncryptDecrypt
    {
        static string passPhrase = "Pas5pr@se";
        static string saltValue = "s@1tValue";
        static string hashAlgorithm = "SHA1";
        static int passwordIterations = 2;
        static string initVector = "@1B2c3D4e5F6g7H8";
        static int keySize = 256;


        public static string EncryptText(string text)
        {

            byte[] initVectorBytes = Encoding.ASCII.GetBytes(initVector);
            byte[] saltValueBytes = Encoding.ASCII.GetBytes(saltValue);

            byte[] plainTextBytes = Encoding.UTF8.GetBytes(text);

            PasswordDeriveBytes password = new PasswordDeriveBytes(
                                                            passPhrase,
                                                            saltValueBytes,
                                                            hashAlgorithm,
                                                            passwordIterations);

            byte[] keyBytes = password.GetBytes(keySize / 8);


            RijndaelManaged symmetricKey = new RijndaelManaged();


            symmetricKey.Mode = CipherMode.CBC;


            ICryptoTransform encryptor = symmetricKey.CreateEncryptor(
                                                             keyBytes,
                                                             initVectorBytes);


            MemoryStream memoryStream = new MemoryStream();


            CryptoStream cryptoStream = new CryptoStream(memoryStream,
                                                         encryptor,
                                                         CryptoStreamMode.Write);

            cryptoStream.Write(plainTextBytes, 0, plainTextBytes.Length);


            cryptoStream.FlushFinalBlock();


            byte[] cipherTextBytes = memoryStream.ToArray();


            memoryStream.Close();
            cryptoStream.Close();


            string decryptText = Convert.ToBase64String(cipherTextBytes);


            return decryptText;
        }

        public static string DecryptText(string encryptText)
        {

            byte[] initVectorBytes = Encoding.ASCII.GetBytes(initVector);
            byte[] saltValueBytes = Encoding.ASCII.GetBytes(saltValue);
            byte[] cipherTextBytes = Convert.FromBase64String(encryptText);

            PasswordDeriveBytes password = new PasswordDeriveBytes(
                                                            passPhrase,
                                                            saltValueBytes,
                                                            hashAlgorithm,
                                                            passwordIterations);

            byte[] keyBytes = password.GetBytes(keySize / 8);

            RijndaelManaged symmetricKey = new RijndaelManaged();

            symmetricKey.Mode = CipherMode.CBC;


            ICryptoTransform decryptor = symmetricKey.CreateDecryptor(
                                                             keyBytes,
                                                             initVectorBytes);

            MemoryStream memoryStream = new MemoryStream(cipherTextBytes);


            CryptoStream cryptoStream = new CryptoStream(memoryStream,
                                                          decryptor,
                                                          CryptoStreamMode.Read);


            byte[] plainTextBytes = new byte[cipherTextBytes.Length];


            int decryptedByteCount = cryptoStream.Read(plainTextBytes,
                                                       0,
                                                       plainTextBytes.Length);


            memoryStream.Close();
            cryptoStream.Close();


            string text = Encoding.UTF8.GetString(plainTextBytes,
                                                       0,
                                                       decryptedByteCount);


            return text;

        }
    }
}
