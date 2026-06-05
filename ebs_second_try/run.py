
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from evaluator import SystemEvaluator

if __name__ == "__main__":
    print("\n" + "=" * 70)
    print(" SISTEM PUBLISH/SUBSCRIBE CU FILTRARE PE CONTINUT")
    print("=" * 70)

    try:
        evaluator = SystemEvaluator()
        evaluator.run_evaluation()

        print("\n" + "=" * 70)
        print(" ✓ EVALUARE COMPLETĂ - SISTEM FUNCȚIONAL")
        print("=" * 70)

    except KeyboardInterrupt:
        print("\n\nTest întrerupt de utilizator")
    except Exception as e:
        print(f"\nEroare: {e}")
        import traceback

        traceback.print_exc()

    input("\nApasati Enter pentru a inchide...")