package group.bda.federate.security.he;

import edu.alibaba.mpc4j.crypto.fhe.Ciphertext;
import edu.alibaba.mpc4j.crypto.fhe.Decryptor;
import edu.alibaba.mpc4j.crypto.fhe.Encryptor;
import edu.alibaba.mpc4j.crypto.fhe.Evaluator;
import edu.alibaba.mpc4j.crypto.fhe.KeyGenerator;
import edu.alibaba.mpc4j.crypto.fhe.Plaintext;
import edu.alibaba.mpc4j.crypto.fhe.PublicKey;
import edu.alibaba.mpc4j.crypto.fhe.SecretKey;
import edu.alibaba.mpc4j.crypto.fhe.context.EncryptionParameters;
import edu.alibaba.mpc4j.crypto.fhe.context.SchemeType;
import edu.alibaba.mpc4j.crypto.fhe.context.SealContext;
import edu.alibaba.mpc4j.crypto.fhe.modulus.CoeffModulus;
import edu.alibaba.mpc4j.crypto.fhe.modulus.CoeffModulus.SecLevelType;
import edu.alibaba.mpc4j.crypto.fhe.modulus.Modulus;
import edu.alibaba.mpc4j.crypto.fhe.zq.UintCore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PHE {

  public static SealContext context = sealContextInit();

  private static final Logger LOG = LogManager.getLogger(PHE.class);

  public static SealContext sealContextInit() {
    EncryptionParameters parms = new EncryptionParameters(SchemeType.BFV);
    long value = (1L << 60) - 1L;
    int polyModulusDegree = 64;
    int[] bitSize = new int[] {60, 60, 60, 60};
    Modulus plainModulus = new Modulus(value);
    parms.setPolyModulusDegree(polyModulusDegree);
    parms.setPlainModulus(plainModulus);
    parms.setCoeffModulus(CoeffModulus.create(polyModulusDegree, bitSize));
    SealContext context = new SealContext(parms, false, SecLevelType.NONE);
    return context;
  }

  public static KeyGenerator keyGenerator() {
    KeyGenerator keygen = new KeyGenerator(context);
    return keygen;
  }

  public static PublicKey generatePublicKey(KeyGenerator keygen) {
    PublicKey pk = new PublicKey();
    keygen.createPublicKey(pk);
    return pk;
  }

  public static Ciphertext add(Ciphertext x1, Ciphertext x2) {
    Evaluator evaluator = new Evaluator(PHE.context);

    Ciphertext output = new Ciphertext();
    evaluator.add(x1, x2, output);
    return output;
  }

  public static Ciphertext multiply(Ciphertext x1, Ciphertext x2) {
    Evaluator evaluator = new Evaluator(PHE.context);

    Ciphertext output = new Ciphertext();
    evaluator.multiply(x1, x2, output);
    return output;
  }

  public static Ciphertext encrypt(PublicKey pk, Plaintext plain) {
    Encryptor encryptor = new Encryptor(context, pk);
    return encryptor.encrypt(plain);
  }

  public static Ciphertext encryptLong(PublicKey pk, long value) {
    Encryptor encryptor = new Encryptor(context, pk);
    Plaintext plaintextValue = new Plaintext(UintCore.uintToHexString(new long[] {value}, 1));
    return encryptor.encrypt(plaintextValue);
  }

  public static Ciphertext[] encryptLong(PublicKey pk, long[] values) {
    Encryptor encryptor = new Encryptor(context, pk);
    Ciphertext[] encrypts = new Ciphertext[values.length];
    for (int i = 0; i < values.length; i++) {
      Plaintext plaintextValue = new Plaintext(UintCore.uintToHexString(new long[] {values[i]}, 1));
      encrypts[i] = encryptor.encrypt(plaintextValue);
    }
    return encrypts;
  }

  public static Plaintext decrypt(SecretKey secretKey, Ciphertext encrypt) {
    Decryptor decryptor = new Decryptor(context, secretKey);
    Plaintext plain = new Plaintext();
    decryptor.decrypt(encrypt, plain);
    return plain;
  }

  public static Plaintext[] decrypt(SecretKey secretKey, Ciphertext[] encrypts) {
    Decryptor decryptor = new Decryptor(context, secretKey);
    Plaintext[] plains = new Plaintext[encrypts.length];
    for (int i = 0; i < encrypts.length; i++) {
      decryptor.decrypt(encrypts[i], plains[i]);
    }
    return plains;
  }

  public static long decryptLong(SecretKey secretKey, Ciphertext encrypt) {
    Decryptor decryptor = new Decryptor(context, secretKey);
    Plaintext plain = new Plaintext();
    decryptor.decrypt(encrypt, plain);
    long decryptValue = Long.parseLong(plain.toString(), 16);
    return decryptValue;
  }

  public static long[] decryptLong(SecretKey secretKey, Ciphertext[] encrypts) {
    Decryptor decryptor = new Decryptor(context, secretKey);
    Plaintext[] plains = new Plaintext[encrypts.length];
    long[] decryptValues = new long[encrypts.length];
    for (int i = 0; i < encrypts.length; i++) {
      plains[i] = new Plaintext();
      decryptor.decrypt(encrypts[i], plains[i]);
      decryptValues[i] = Long.parseLong(plains[i].toString(), 16);
      if (i % 1000 == 0) {
        LOG.info("decrypt {} rows...", i);
      }
    }
    return decryptValues;
  }

  public static Ciphertext distance(Ciphertext encryptedX1, Ciphertext encryptedY1,
      Plaintext plaintextX2, Plaintext plaintextY2) {
    Evaluator evaluator = new Evaluator(PHE.context);
    Ciphertext encryptedSub1 = new Ciphertext();
    evaluator.subPlain(encryptedX1, plaintextX2, encryptedSub1);

    evaluator.square(encryptedSub1, encryptedSub1);

    Ciphertext encryptedSub2 = new Ciphertext();
    evaluator.subPlain(encryptedY1, plaintextY2, encryptedSub2);

    evaluator.square(encryptedSub2, encryptedSub2);
    Ciphertext encryptedDistance = new Ciphertext();
    evaluator.add(encryptedSub1, encryptedSub2, encryptedDistance);
    return encryptedDistance;
  }

  public static Ciphertext polyDistance(Plaintext a, Plaintext b, Ciphertext encryptedX1,
      Ciphertext encryptedY1, Plaintext plaintextX2, Plaintext plaintextY2) {
    Evaluator evaluator = new Evaluator(PHE.context);

    Ciphertext encryptedSub1 = new Ciphertext();
    evaluator.subPlain(encryptedX1, plaintextX2, encryptedSub1);

    evaluator.square(encryptedSub1, encryptedSub1);

    Ciphertext encryptedSub2 = new Ciphertext();
    evaluator.subPlain(encryptedY1, plaintextY2, encryptedSub2);

    evaluator.square(encryptedSub2, encryptedSub2);
    Ciphertext encryptedDistance = new Ciphertext();
    evaluator.add(encryptedSub1, encryptedSub2, encryptedDistance);
    evaluator.multiplyPlain(encryptedDistance, a, encryptedDistance);
    evaluator.addPlain(encryptedDistance, b, encryptedDistance);
    return encryptedDistance;
  }

  public static Ciphertext poly(Plaintext a, Plaintext b, Ciphertext x) {
    Evaluator evaluator = new Evaluator(PHE.context);
    Ciphertext encryptedSub1 = new Ciphertext();
    evaluator.multiplyPlain(x, a, encryptedSub1);
    evaluator.addPlain(encryptedSub1, b, encryptedSub1);
    return encryptedSub1;
  }
  public static Ciphertext[] distance(Ciphertext encryptedX1, Ciphertext encryptedY1,
      Plaintext[] plaintextX2, Plaintext[] plaintextY2) {
    Evaluator evaluator = new Evaluator(PHE.context);
    Ciphertext[] encryptedDistances = new Ciphertext[plaintextX2.length];
    for (int i = 0; i < plaintextX2.length; i++) {
      Ciphertext encryptedSub1 = new Ciphertext();
      evaluator.subPlain(encryptedX1, plaintextX2[i], encryptedSub1);
      evaluator.square(encryptedSub1, encryptedSub1);
      Ciphertext encryptedSub2 = new Ciphertext();
      evaluator.subPlain(encryptedY1, plaintextY2[i], encryptedSub2);
      evaluator.square(encryptedSub2, encryptedSub2);
      encryptedDistances[i] = new Ciphertext();
      evaluator.add(encryptedSub1, encryptedSub2, encryptedDistances[i]);
    }
    return encryptedDistances;
  }

  public static Ciphertext distance(Ciphertext encryptedX1, Ciphertext encryptedY1, long x2,
      long y2) {
    Plaintext plaintextX2 = new Plaintext(UintCore.uintToHexString(new long[] {x2}, 1));
    Plaintext plaintextY2 = new Plaintext(UintCore.uintToHexString(new long[] {y2}, 1));
    return distance(encryptedX1, encryptedY1, plaintextX2, plaintextY2);
  }

  public static Ciphertext[] distance(Ciphertext encryptedX1, Ciphertext encryptedY1, long[] x2,
      long[] y2) {
    Plaintext[] plaintextX2 = new Plaintext[x2.length];
    Plaintext[] plaintextY2 = new Plaintext[y2.length];
    for (int i = 0; i < x2.length; i++) {
      plaintextX2[i] = new Plaintext(UintCore.uintToHexString(new long[] {x2[i]}, 1));
      plaintextY2[i] = new Plaintext(UintCore.uintToHexString(new long[] {y2[i]}, 1));
    }
    return distance(encryptedX1, encryptedY1, plaintextX2, plaintextY2);
  }

  public static Ciphertext[] polyDistance(Plaintext a, Plaintext b, Ciphertext encryptedX1,
      Ciphertext encryptedY1, long[] x2,
      long[] y2) {
    Plaintext[] plaintextX2 = new Plaintext[x2.length];
    Plaintext[] plaintextY2 = new Plaintext[y2.length];
    Ciphertext[] result = new Ciphertext[x2.length];
    for (int i = 0; i < x2.length; i++) {
      plaintextX2[i] = new Plaintext(UintCore.uintToHexString(new long[] {x2[i]}, 1));
      plaintextY2[i] = new Plaintext(UintCore.uintToHexString(new long[] {y2[i]}, 1));
      result[i] = polyDistance(a, b, encryptedX1, encryptedY1, plaintextX2[i], plaintextY2[i]);
      if (i % 1000 == 0) {
        LOG.info("compute {} rows in encryption mode...", i);
      }
    }
    return result;
  }

  public static void main(String[] args) {

    // New a keygen
    KeyGenerator keygen = keyGenerator();
    // Generate a public key
    PublicKey publicKey = generatePublicKey(keygen);
    Encryptor encryptor = new Encryptor(context, publicKey);

    long a = 4L;
    long b = 400L;
    long r = 127093L * 127093L;
    Plaintext plainR = new Plaintext(UintCore.uintToHexString(new long[] {r}, 1));
    Ciphertext encRadius = new Ciphertext();
    encryptor.encrypt(plainR, encRadius);


    Plaintext plaintextA = new Plaintext(UintCore.uintToHexString(new long[] {a}, 1));
    Plaintext plaintextB = new Plaintext(UintCore.uintToHexString(new long[] {b}, 1));
    Ciphertext ciphertextPoly = PHE.poly(plaintextA, plaintextB, encRadius);

    // Use keygen to decrypt
    long actual = decryptLong(keygen.secretKey(), ciphertextPoly);
    long expected = a * r + b;
    System.out.println(actual);
    System.out.println(expected);
    String res = (actual - r * r) < 0 ? "In" : "Out";
    System.out.println("Result :" + res);

  }

}
