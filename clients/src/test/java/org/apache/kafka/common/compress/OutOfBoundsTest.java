package org.apache.kafka.common.compress;

/*
 * Copyright 2025 Jonas Konrad and the lz4-java contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.lz4.LZ4SafeDecompressor;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class OutOfBoundsTest {
    private static Stream<LZ4Factory> lz4Factories() {
        return Stream.of(
                LZ4Factory.fastestInstance()
//                LZ4Factory.fastestJavaInstance(),
                // LZ4Factory.nativeInsecureInstance(),
//                LZ4Factory.nativeInstance(),
//                LZ4Factory.safeInstance()
//                LZ4Factory.unsafeInsecureInstance()
        );
    }

    static Stream<LZ4FastDecompressor> fastDecompressors() {
        return lz4Factories().map(LZ4Factory::fastDecompressor);
    }

    private static Stream<LZ4SafeDecompressor> safeDecompressors() {
        return lz4Factories().map(LZ4Factory::safeDecompressor);
    }

    /**
     * Abstraction over {@link LZ4FastDecompressor} and {@link LZ4SafeDecompressor}.
     *
     * <p>Should only be used for decompression which is expected to fail, because for the non-failing case
     * {@link LZ4FastDecompressor} and {@link LZ4SafeDecompressor} behave differently regarding whether the
     * input or output should be fully consumed.
     */
    public interface FallibleDecompressor {
        void decompress(byte[] input, byte[] output) throws LZ4Exception;

        static FallibleDecompressor fromFast(LZ4FastDecompressor fastDecompressor) {
            return new FallibleDecompressor() {
                @Override
                public void decompress(byte[] src, byte[] dest) throws LZ4Exception {
                    fastDecompressor.decompress(src, dest);
                }

                @Override
                public String toString() {
                    return fastDecompressor.toString();
                }
            };
        }

        static FallibleDecompressor fromSafe(LZ4SafeDecompressor safeDecompressor) {
            return new FallibleDecompressor() {
                @Override
                public void decompress(byte[] src, byte[] dest) throws LZ4Exception {
                    safeDecompressor.decompress(src, dest);
                }

                @Override
                public String toString() {
                    return safeDecompressor.toString();
                }
            };
        }
    }

    static Stream<FallibleDecompressor> allDecompressors() {
//        return Stream.concat(fastDecompressors().map(FallibleDecompressor::fromFast),
//                safeDecompressors().map(FallibleDecompressor::fromSafe));
        return safeDecompressors().map(FallibleDecompressor::fromSafe);
    }

    @ParameterizedTest
    @MethodSource("allDecompressors")
    public void incompleteInput(FallibleDecompressor decompressor) {
        byte[] input = {
                (byte) 0xf0,
                -1, -1, -1, -1, -1, -1, -1, -1, 0
        };
        byte[] output = new byte[2055];
        assertThrows(LZ4Exception.class, () -> decompressor.decompress(input, output));
    }

    @ParameterizedTest
    @MethodSource("fastDecompressors")
    public void beyondBufferCapacity(LZ4FastDecompressor fastDecompressor) {
        byte[] compressed = {
                // one frame with 4x literal 0x77 and a copy of the same
                (byte) 0x40,
                0x77, 0x77, 0x77, 0x77,
                0x04,
                0x00,
                // one frame with 8x literal 0x66
                (byte) 0x80,
                0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66,
                0x00,
                0x00
        };
        byte[] output = new byte[16];

        // normal decompression. so far so good.
        fastDecompressor.decompress(ByteBuffer.wrap(compressed), ByteBuffer.wrap(output));
        assertEquals(0x77, output[0]);
        assertEquals(0x66, output[8]);

        // but if we only pass half the input size, we should get an error
        assertThrows(LZ4Exception.class, () -> fastDecompressor.decompress(ByteBuffer.wrap(compressed, 0, 7).slice(), 0, ByteBuffer.wrap(output), 0, 16));
    }

    // Note: For JNI decompressor this might not actually overflow because it uses larger variable types,
    // but instead fails because the input is incomplete
    @ParameterizedTest
    @MethodSource("allDecompressors")
    public void literalLenOverflow(FallibleDecompressor decompressor) {
        ByteArrayOutputStream inputWriter = new ByteArrayOutputStream();
        // Token
        inputWriter.write((byte) 0b1111_0000);
        // Causes overflow for `literalLen`
        byte[] literalLenBytes = new byte[Integer.MAX_VALUE / 255 + 1]; // ~9MB
        Arrays.fill(literalLenBytes, (byte) 255);
        inputWriter.writeBytes(literalLenBytes);
        inputWriter.write(1);

        inputWriter.writeBytes(new byte[20]);

        byte[] input = inputWriter.toByteArray();
        byte[] output = new byte[2055];
        assertThrows(LZ4Exception.class, () -> decompressor.decompress(input, output));
    }

    // Note: For JNI decompressor this might not actually overflow because it uses larger variable types,
    // but instead fails because the input is incomplete
    @ParameterizedTest
    @MethodSource("allDecompressors")
    public void matchLenOverflow(FallibleDecompressor decompressor) {
        ByteArrayOutputStream inputWriter = new ByteArrayOutputStream();
        // Token
        inputWriter.write((byte) 0b0000_1111);
        // matchDec
        inputWriter.writeBytes(new byte[2]);

        // Causes overflow for `matchLen`
        byte[] matchLenBytes = new byte[Integer.MAX_VALUE / 255 + 1]; // ~9MB
        Arrays.fill(matchLenBytes, (byte) 255);
        inputWriter.writeBytes(matchLenBytes);
        inputWriter.write(1);

        // `matchLen` overflow only causes out-of-bounds access during next iteration

        // Token
        inputWriter.write(255);
        // Arbitrary literal data
        inputWriter.writeBytes(new byte[1000]);

        byte[] input = inputWriter.toByteArray();
        byte[] output = new byte[2055];
        assertThrows(LZ4Exception.class, () -> decompressor.decompress(input, output));
    }
}
