# import pandas as pd
#
# from AnalyticsCache.handle_rules import generate_features, predict_outcome
# from SampleScrubber.cleaner_model import Uniop
#
# # 创建一个包含错误的简单数据集
# data = {
#     'name': ['apple', 'banana', 'chrry', 'date', 'elderberry'],
#     'color': ['red', 'yellow', 'red', 'brown', 'purple']
# }
# df = pd.DataFrame(data)
#
# # 定义一些已知的正确和错误的清洗操作
# # 假设我们知道'chrry'是一个拼写错误，应该更正为'cherry'
# correct_operations = {
#     Uniop('name', 'cherry', ('name', {'chrry'}), 'correct spelling')
# }
#
# # 假设错误地更改'banana'为'cherry'是不正确的操作
# incorrect_operations = {
#     Uniop('name', 'cherry', ('name', {'banana'}), 'incorrect change')
# }
#
# # 训练模型
# trained_model = generate_features(incorrect_operations, correct_operations, df)
#
# # 验证模型
# if trained_model is not None:
#     # 定义一些新的测试操作
#     test_operations = [
#         Uniop('name', 'cherry', ('name', {'chrry'}), 'test correct spelling'),
#         Uniop('name', 'cherry', ('name', {'banana'}), 'test incorrect change'),
#         Uniop('name', 'apple', ('name', {'aple'}), 'test new operation')
#     ]
#
#     for test_op in test_operations:
#         prediction = predict_outcome(trained_model, test_op, df)
#         print(f"Prediction for operation '{test_op.name}': {'Correct' if prediction else 'Incorrect'}")
# else:
#     print("Model training failed or returned None.")
