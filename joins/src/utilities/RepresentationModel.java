package utilities;

import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import java.util.HashSet;
import java.util.Set;
import org.scify.jedai.datamodel.Attribute;
import org.scify.jedai.datamodel.EntityProfile;

/**
 *
 * @author gap2
 */
public class RepresentationModel {
    
    public static String getAttributeValue(EntityProfile e) {
        StringBuilder sb = new StringBuilder();
        for (Attribute a : e.getAttributes()) {
            sb.append(a.getValue()).append("\t");
        }
        return sb.toString();
    }

    public static Set<String> getCharNGrams(int n, String value) {
        final Set<String> nGrams = new HashSet<>();
        if (value.length() < n) {
            if (!value.isBlank()) {
                nGrams.add(value);
            }
        } else {
            int currentPosition = 0;
            final int length = value.length() - (n - 1);
            while (currentPosition < length) {
                String ngram = value.substring(currentPosition, currentPosition + n);
                if (!ngram.isBlank()) {
                    nGrams.add(ngram);
                }
                currentPosition++;
            }
        }
        return nGrams;
    }

    public static Set<String> getCharNGramsMultiset(int n, String value) {
        final Set<String> nGrams = new HashSet<>();
        if (value.length() < n) {
            if (!value.isBlank()) {
                nGrams.add(value + "0");
            }
        } else {
            int currentPosition = 0;
            final int length = value.length() - (n - 1);
            final TObjectIntMap tokenCounter = new TObjectIntHashMap();
            while (currentPosition < length) {
                String ngram = value.substring(currentPosition, currentPosition + n);
                if (!ngram.isBlank()) {
                    int id = tokenCounter.get(ngram);
                    nGrams.add(ngram + id);
                    id++;
                    tokenCounter.put(ngram, id);
                }
                currentPosition++;
            }
        }
        return nGrams;
    }

    public static Set<String> tokenizeEntity(String value, Tokenizer t) {
        Set<String> tokens = new HashSet<>();
        final String normalizedValue = value.toLowerCase().trim();
        switch (t) {
            case CHARACTER_BIGRAMS:
                tokens = getCharNGrams(2, normalizedValue);
                break;
            case CHARACTER_BIGRAMS_MULTISET:
                tokens = getCharNGramsMultiset(2, normalizedValue);
                break;
            case CHARACTER_TRIGRAMS:
                tokens = getCharNGrams(3, normalizedValue);
                break;
            case CHARACTER_TRIGRAMS_MULTISET:
                tokens = getCharNGramsMultiset(3, normalizedValue);
                break;
            case CHARACTER_FOURGRAMS:
                tokens = getCharNGrams(4, normalizedValue);
                break;
            case CHARACTER_FOURGRAMS_MULTISET:
                tokens = getCharNGramsMultiset(4, normalizedValue);
                break;
            case CHARACTER_FIVEGRAMS:
                tokens = getCharNGrams(5, normalizedValue);
                break;
            case CHARACTER_FIVEGRAMS_MULTISET:
                tokens = getCharNGramsMultiset(5, normalizedValue);
                break;
            case WHITESPACE:
                String[] words = normalizedValue.split("[\\W_]");
                for (String w : words) {
                    if (!w.isBlank()) {
                        tokens.add(w);
                    }
                }
                break;
            case WHITESPACE_MULTISET:
                final TObjectIntMap tokenCounter = new TObjectIntHashMap();
                String[] tokenList = normalizedValue.split("[\\W_]");
                for (String tk : tokenList) {
                    if (!tk.isBlank()) {
                        int id = tokenCounter.get(tk);
                        tokens.add(tk + id);
                        id++;
                        tokenCounter.put(tk, id);
                    }
                }
                break;
        }
        return tokens;
    }
}
